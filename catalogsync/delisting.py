"""Snapshot-scoped delisting.

A *complete* snapshot -- every page committed and no rejected rows -- is the
only evidence strong enough to declare that an identity absent from it has left
the catalogue.  Such a snapshot records a monotonic *barrier* per source: the
source version boundary together with the set of SKUs the snapshot returned.

Effective state is derived, never guessed:

* an identity stored with ``revision <= boundary`` and absent from the
  barrier's membership is delisted, even if a late record for it arrives later;
* an identity stored with ``revision > boundary`` is newer than the snapshot
  and is retained (the snapshot cannot speak for versions past its cut);
* an identity present in the membership is effective.

Re-deriving the state from the stored product row makes pagination resume,
duplicate processing and reconciliation retries converge on the same result.
"""

from .models import canonical, now


class Delisting:
    def barrier(self, connection, source_id):
        return connection.execute(
            "SELECT * FROM snapshot_barriers WHERE source_id = ?", (source_id,)
        ).fetchone()

    def membership(self, connection, run_id, sku):
        return connection.execute(
            "SELECT 1 FROM snapshot_items WHERE run_id = ? AND sku = ?", (run_id, sku)
        ).fetchone() is not None

    def record_item(self, connection, run_id, source_id, sku, revision):
        """Remember that a snapshot run returned ``sku`` (membership only)."""
        connection.execute(
            """INSERT INTO snapshot_items (run_id, source_id, sku, revision)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(run_id, sku) DO UPDATE SET revision = excluded.revision
            WHERE excluded.revision >= snapshot_items.revision""",
            (run_id, source_id, sku, revision),
        )

    def state(self, connection, source_id, sku):
        return connection.execute(
            "SELECT * FROM delist_states WHERE source_id = ? AND sku = ?",
            (source_id, sku),
        ).fetchone()

    def sync(self, connection, source_id, sku, prior_known=True):
        """Derive the effective state of one identity from its stored row."""
        product = connection.execute(
            "SELECT revision, fingerprint FROM products WHERE source_id = ? AND sku = ?",
            (source_id, sku),
        ).fetchone()
        if product is None:
            return "absent"
        barrier = self.barrier(connection, source_id)
        if barrier is None or product["revision"] > barrier["boundary"]:
            return self._clear(connection, source_id, sku)
        if self.membership(connection, barrier["run_id"], sku):
            return self._clear(connection, source_id, sku)
        prior_revision = product["revision"] if prior_known else None
        prior_fingerprint = product["fingerprint"] if prior_known else None
        return self._delist(
            connection,
            source_id,
            sku,
            barrier,
            prior_revision,
            prior_fingerprint,
        )

    def _clear(self, connection, source_id, sku):
        removed = connection.execute(
            "DELETE FROM delist_states WHERE source_id = ? AND sku = ?", (source_id, sku)
        ).rowcount
        if removed:
            connection.execute(
                """INSERT INTO audit_events (source_id, run_id, action, detail, created_at)
                VALUES (?, NULL, 'product_relisted', ?, ?)""",
                (source_id, canonical({"sku": sku}), now()),
            )
        return "effective"

    def _delist(self, connection, source_id, sku, barrier, revision, fingerprint):
        existing = self.state(connection, source_id, sku)
        same_basis = (
            existing is not None
            and existing["run_id"] == barrier["run_id"]
            and existing["boundary"] == barrier["boundary"]
        )
        if not same_basis:
            connection.execute(
                """INSERT INTO delist_states
                (source_id, sku, run_id, snapshot_id, boundary, delisted_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id, sku) DO UPDATE SET
                    run_id = excluded.run_id,
                    snapshot_id = excluded.snapshot_id,
                    boundary = excluded.boundary,
                    delisted_at = excluded.delisted_at
                WHERE excluded.boundary >= delist_states.boundary""",
                (
                    source_id,
                    sku,
                    barrier["run_id"],
                    barrier["snapshot_id"],
                    barrier["boundary"],
                    now(),
                ),
            )
        evidence = connection.execute(
            """INSERT OR IGNORE INTO delistings
            (source_id, sku, run_id, snapshot_id, boundary,
             prior_revision, prior_fingerprint, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                source_id,
                sku,
                barrier["run_id"],
                barrier["snapshot_id"],
                barrier["boundary"],
                revision,
                fingerprint,
                now(),
            ),
        )
        if not same_basis and evidence.rowcount:
            connection.execute(
                """INSERT INTO audit_events (source_id, run_id, action, detail, created_at)
                VALUES (?, ?, 'product_delisted', ?, ?)""",
                (
                    source_id,
                    barrier["run_id"],
                    canonical({
                        "sku": sku,
                        "snapshot_id": barrier["snapshot_id"],
                        "boundary": barrier["boundary"],
                    }),
                    now(),
                ),
            )
        return "delisted"

    def finalize(self, connection, run):
        """Apply a completing snapshot as the authoritative absence boundary.

        Runs inside the transaction that marks the run completed, so a retry
        after an interruption re-derives identical effective state.
        """
        if run["mode"] != "snapshot":
            return None
        if run["status"] != "running" or run["next_cursor"] is not None:
            return None
        if run["snapshot_id"] is None or run["boundary"] is None:
            return None
        # A snapshot containing rejected rows is incomplete evidence: it may
        # swallow a real product, so it never declares absence.
        if run["rejected"] != 0:
            return None
        source_id = run["source_id"]
        barrier = self.barrier(connection, source_id)
        if barrier is not None:
            if barrier["run_id"] == run["id"]:
                return None
            if run["boundary"] < barrier["boundary"]:
                # The cut point would move backwards; an older snapshot cannot
                # override a newer barrier's membership.
                return None
            if run["boundary"] == barrier["boundary"]:
                # Same cut point: only a run created after the barrier's run may
                # replace it, so a stale replay cannot flip the result.
                previous = connection.execute(
                    "SELECT created_at FROM runs WHERE id = ?", (barrier["run_id"],)
                ).fetchone()
                if previous is not None and run["created_at"] <= previous["created_at"]:
                    return None
        connection.execute(
            """INSERT INTO snapshot_barriers
            (source_id, run_id, snapshot_id, boundary, recorded_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
                run_id = excluded.run_id,
                snapshot_id = excluded.snapshot_id,
                boundary = excluded.boundary,
                recorded_at = excluded.recorded_at
            WHERE excluded.boundary > snapshot_barriers.boundary
                OR (excluded.boundary = snapshot_barriers.boundary
                    AND excluded.run_id IS NOT snapshot_barriers.run_id)""",
            (source_id, run["id"], run["snapshot_id"], run["boundary"], now()),
        )
        for product in connection.execute(
            "SELECT sku FROM products WHERE source_id = ?", (source_id,)
        ).fetchall():
            self.sync(connection, source_id, product["sku"])
        return run["boundary"]
