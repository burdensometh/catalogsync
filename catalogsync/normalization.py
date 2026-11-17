from copy import deepcopy
from decimal import Decimal, InvalidOperation
import json
import re
import unicodedata

from .errors import ValidationError


MISSING = object()
TARGETS = {
    "sku", "revision", "title", "category", "currency", "price",
    "stock", "active", "attributes",
}
OPERATIONS = {
    "trim", "lower", "upper", "nfkc", "integer", "boolean",
    "minor_units", "replace", "lookup", "decimal", "prefix", "suffix",
}


def at_path(record, path):
    if path == "$":
        return record
    current = record
    for component in path.split("."):
        if isinstance(current, dict):
            if component not in current:
                return MISSING
            current = current[component]
        elif isinstance(current, list) and component.isdigit():
            index = int(component)
            if index >= len(current):
                return MISSING
            current = current[index]
        else:
            return MISSING
    return current


def validate_path(value):
    if not isinstance(value, str) or not value or len(value) > 300:
        raise ValidationError("field path must be a nonempty string of <=300 characters")
    if value == "$":
        return value
    components = value.split(".")
    if any(not component or len(component) > 80 for component in components):
        raise ValidationError("invalid field path component")
    if len(components) > 20:
        raise ValidationError("field path nesting exceeds 20")
    return value


def validate_operation(step):
    if isinstance(step, str):
        step = {"op": step}
    if not isinstance(step, dict) or step.get("op") not in OPERATIONS:
        raise ValidationError("unsupported normalization operation")
    operation = step["op"]
    if operation in {"prefix", "suffix"}:
        if not isinstance(step.get("value"), str):
            raise ValidationError("prefix/suffix requires a string value")
    if operation == "replace":
        if not isinstance(step.get("old"), str) or not isinstance(step.get("new"), str):
            raise ValidationError("replace requires old and new strings")
        if not step["old"]:
            raise ValidationError("replace old cannot be empty")
    if operation == "lookup":
        if not isinstance(step.get("values"), dict):
            raise ValidationError("lookup requires an object of values")
    if operation == "minor_units":
        digits = step.get("digits", 2)
        if type(digits) is not int or not 0 <= digits <= 6:
            raise ValidationError("minor unit digits must be between 0 and 6")
    return deepcopy(step)


def transform(value, step):
    operation = step["op"]
    if operation in {"trim", "lower", "upper", "nfkc", "replace", "prefix", "suffix"}:
        if not isinstance(value, str):
            raise ValidationError(f"{operation} requires text")
        if operation == "trim":
            return value.strip()
        if operation == "lower":
            return value.lower()
        if operation == "upper":
            return value.upper()
        if operation == "nfkc":
            return unicodedata.normalize("NFKC", value)
        if operation == "replace":
            return value.replace(step["old"], step["new"])
        if operation == "prefix":
            return step["value"] + value
        return value + step["value"]
    if operation == "integer":
        if type(value) is int:
            return value
        if not isinstance(value, str) or not re.fullmatch(r"[+-]?\d+", value.strip()):
            raise ValidationError("integer conversion requires an integer representation")
        return int(value.strip())
    if operation == "boolean":
        if type(value) is bool:
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "yes", "1"}:
                return True
            if normalized in {"false", "no", "0"}:
                return False
        raise ValidationError("boolean conversion requires an unambiguous value")
    if operation in {"decimal", "minor_units"}:
        if type(value) not in {int, str}:
            raise ValidationError("decimal normalization requires integer or text")
        try:
            decimal = Decimal(value)
        except InvalidOperation:
            raise ValidationError("cannot normalize decimal") from None
        if not decimal.is_finite():
            raise ValidationError("decimal must be finite")
        if operation == "minor_units":
            decimal /= Decimal(10) ** step.get("digits", 2)
        return format(decimal, "f")
    if operation == "lookup":
        key = str(value)
        if key in step["values"]:
            return deepcopy(step["values"][key])
        if "default" in step:
            return deepcopy(step["default"])
        raise ValidationError(f"no lookup mapping for {key!r}")
    raise ValidationError("unsupported transformation")


class Profile:
    def __init__(self, specification=None):
        specification = specification or {}
        if not isinstance(specification, dict):
            raise ValidationError("profile must be an object")
        allowed = {"fields", "defaults", "quality", "parser"}
        if set(specification) - allowed:
            raise ValidationError("unknown profile properties")
        self.parser = specification.get("parser", "json")
        if self.parser not in {"json", "html", "jsonld"}:
            raise ValidationError("parser must be json, html, or jsonld")
        self.defaults = deepcopy(specification.get("defaults", {}))
        if not isinstance(self.defaults, dict) or set(self.defaults) - TARGETS:
            raise ValidationError("defaults contain unsupported product fields")
        fields = specification.get("fields", {})
        if not isinstance(fields, dict) or set(fields) - TARGETS:
            raise ValidationError("field mappings contain unsupported product fields")
        self.fields = {}
        for target, mapping in fields.items():
            if isinstance(mapping, str):
                mapping = {"path": mapping}
            if not isinstance(mapping, dict):
                raise ValidationError("field mapping must be a path or object")
            if set(mapping) - {"path", "default", "steps"}:
                raise ValidationError("unknown field mapping property")
            path = validate_path(mapping.get("path", target))
            steps = mapping.get("steps", [])
            if not isinstance(steps, list) or len(steps) > 10:
                raise ValidationError("normalization steps must be an array of <=10 operations")
            output = {"path": path, "steps": [validate_operation(step) for step in steps]}
            if "default" in mapping:
                output["default"] = deepcopy(mapping["default"])
            self.fields[target] = output
        from .quality import QualityPolicy
        self.quality = QualityPolicy(specification.get("quality", {}))

    def document(self):
        return {
            "parser": self.parser,
            "fields": deepcopy(self.fields),
            "defaults": deepcopy(self.defaults),
            "quality": self.quality.document(),
        }

    def normalize(self, raw):
        if not isinstance(raw, dict):
            raise ValidationError("source record must be an object")
        result = deepcopy(self.defaults)
        if not self.fields:
            result.update(raw)
        else:
            for target, mapping in self.fields.items():
                value = at_path(raw, mapping["path"])
                if value is MISSING:
                    if "default" in mapping:
                        value = deepcopy(mapping["default"])
                    elif target in result:
                        continue
                    else:
                        continue
                for step in mapping["steps"]:
                    value = transform(value, step)
                result[target] = value
        self.quality.validate(result)
        return result
