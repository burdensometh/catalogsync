import json

from .errors import NotFound, ValidationError
from .models import canonical, digest


class Rejections:
    def __init__(self, application):
        self.app = application

    def get(self, reject_id):
        with self.app.database.read() as connection:
            row = connection.execute(
                """SELECT j.*,r.source_id FROM rejects j
                JOIN runs r ON r.id=j.run_id WHERE j.id=?""", (reject_id,)
            ).fetchone()
            if row is None:
                raise NotFound("rejected record does not exist")
            result = dict(row)
            result["raw_record"] = json.loads(result["raw_record"])
            return result

    def search(self, source_id=None, reason=None, limit=100, offset=0):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValidationError("limit must be between 1 and 1000")
        if type(offset) is not int or offset < 0:
            raise ValidationError("offset must be nonnegative")
        clauses = []
        params = []
        if source_id:
            clauses.append("r.source_id=?")
            params.append(source_id)
        if reason:
            clauses.append("instr(lower(j.reason),lower(?))>0")
            params.append(reason)
        where = " AND ".join(clauses) or "1=1"
        with self.app.database.read() as connection:
            total = connection.execute(
                "SELECT count(*) FROM rejects j JOIN runs r ON r.id=j.run_id WHERE " + where,
                params,
            ).fetchone()[0]
            rows = connection.execute(
                "SELECT j.*,r.source_id FROM rejects j JOIN runs r ON r.id=j.run_id WHERE "
                + where + " ORDER BY j.id LIMIT ? OFFSET ?",
                [*params, limit, offset],
            )
            return {"total": total, "items": [dict(row) for row in rows]}

    def requeue(self, reject_id, corrected=None):
        rejection = self.get(reject_id)
        record = rejection["raw_record"] if corrected is None else corrected
        if not isinstance(record, dict):
            raise ValidationError("corrected source record must be an object")
        event_id = f"reject:{reject_id}:" + digest(record)
        submission = self.app.inbox.submit(rejection["source_id"], [{"id": event_id, "record": record}])
        return {
            "reject_id": reject_id,
            "source_id": rejection["source_id"],
            "event_id": event_id,
            "submission": submission,
            "status": "queued_for_current_profile_at_consumption",
        }

    def export(self, run_id):
        run = self.app.runs.get(run_id)
        rows = self.app.runs.rejects(run_id, 100000)
        return "".join(canonical({
            "reject_id": row["id"],
            "source_id": run["source_id"],
            "run_id": run_id,
            "cursor": row["cursor"],
            "ordinal": row["ordinal"],
            "reason": row["reason"],
            "record": json.loads(row["raw_record"]),
        }) + "\n" for row in rows)
