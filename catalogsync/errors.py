class CatalogError(Exception):
    code = "catalog_error"


class ValidationError(CatalogError):
    code = "invalid_input"


class NotFound(CatalogError):
    code = "not_found"


class Conflict(CatalogError):
    code = "conflict"


class FetchError(CatalogError):
    code = "fetch_failed"


class IntegrityError(CatalogError):
    code = "integrity_error"
