from prompt_toolkit.validation import ValidationError, Validator


class NumberValidator(Validator):
    def __init__(self, min=0, max=0):
        self._min = min
        self._max = max

    def validate(self, document):
        value = document.text

        try:
            value = int(value)
        except:
            raise ValidationError(
                message=f"({value}) is not a number",
                cursor_position=document.cursor_position,
            )

        if (self._min + self._max) != 0:
            if value > self._max:
                raise ValidationError(
                    message=f"Number is more than {self._max}",
                    cursor_position=document.cursor_position,
                )
            if value < self._min:
                raise ValidationError(
                    message=f"Number is less than {self._min}",
                    cursor_position=document.cursor_position,
                )
