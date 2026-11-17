from decimal import Decimal, InvalidOperation

from .errors import ValidationError


class QualityPolicy:
    def __init__(self, specification):
        if not isinstance(specification, dict):
            raise ValidationError("quality policy must be an object")
        if set(specification) - {"required", "ranges", "choices", "max_lengths"}:
            raise ValidationError("unknown quality policy properties")
        self.required = specification.get("required", [])
        self.ranges = specification.get("ranges", {})
        self.choices = specification.get("choices", {})
        self.max_lengths = specification.get("max_lengths", {})
        if not isinstance(self.required, list) or not all(isinstance(x, str) for x in self.required):
            raise ValidationError("required must be an array of field names")
        for value in (self.ranges, self.choices, self.max_lengths):
            if not isinstance(value, dict):
                raise ValidationError("quality rules must be objects")
        for field, bounds in self.ranges.items():
            if not isinstance(bounds, dict) or set(bounds) - {"min", "max"}:
                raise ValidationError("range must contain min and/or max")
            for bound in bounds.values():
                self.number(bound, field)
            if "min" in bounds and "max" in bounds:
                if self.number(bounds["min"], field) > self.number(bounds["max"], field):
                    raise ValidationError("range minimum exceeds maximum")
        for values in self.choices.values():
            if not isinstance(values, list) or not values:
                raise ValidationError("choices must be nonempty arrays")
        for size in self.max_lengths.values():
            if type(size) is not int or size < 1:
                raise ValidationError("max_lengths must contain positive integers")

    def number(self, value, field):
        try:
            if isinstance(value, bool):
                raise InvalidOperation()
            result = Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise ValidationError(f"{field}: expected a numeric quality bound/value") from None
        if not result.is_finite():
            raise ValidationError(f"{field}: nonfinite numeric value")
        return result

    def document(self):
        return {
            "required": self.required,
            "ranges": self.ranges,
            "choices": self.choices,
            "max_lengths": self.max_lengths,
        }

    def validate(self, record):
        for field in self.required:
            if field not in record or record[field] is None or record[field] == "":
                raise ValidationError(f"quality: {field} is required")
        for field, bounds in self.ranges.items():
            if field not in record:
                continue
            value = self.number(record[field], field)
            if "min" in bounds and value < self.number(bounds["min"], field):
                raise ValidationError(f"quality: {field} below allowed minimum")
            if "max" in bounds and value > self.number(bounds["max"], field):
                raise ValidationError(f"quality: {field} above allowed maximum")
        for field, choices in self.choices.items():
            if field in record and record[field] not in choices:
                raise ValidationError(f"quality: {field} outside allowed choices")
        for field, size in self.max_lengths.items():
            if field in record:
                if not isinstance(record[field], str) or len(record[field]) > size:
                    raise ValidationError(f"quality: {field} exceeds text length")
