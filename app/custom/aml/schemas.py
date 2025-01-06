from decimal import Decimal
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    Field,
    OnErrorOmit,
    field_validator,
)


from ...schemas import TronAddress, TronSymbol


def check_split_perc_sum(
    value: dict[TronAddress, Decimal],
) -> dict[TronAddress, Decimal]:
    if sum(value.values()) == 1:
        return value
    raise ValueError(f"{value} sum of split parts shoud be 1")


AmlScore = Annotated[Decimal, Field(ge=0, le=1, multiple_of=0.01)]
PayoutRatio = Annotated[Decimal, Field(gt=0, le=1, multiple_of=0.01)]


class RegularSplitConfig(BaseModel):
    addresses: Annotated[
        dict[TronAddress, PayoutRatio],
        Field(min_length=1),
        AfterValidator(check_split_perc_sum),
    ]


class RegularSplitConfig(BaseModel):
    state: Literal["disabled", "enabled"]
    cryptos: Annotated[
        dict[OnErrorOmit[TronSymbol], RegularSplitConfig], Field(min_length=1)
    ]


class AmlRiskConfig(BaseModel):
    min_value: AmlScore
    max_value: AmlScore
    addresses: Annotated[
        dict[TronAddress, PayoutRatio],
        Field(min_length=1),
        AfterValidator(check_split_perc_sum),
    ]


class AmlCryptoConfig(BaseModel):
    min_check_amount: Decimal
    risk_scores: Annotated[dict[str, AmlRiskConfig], Field(min_length=1)]

    @field_validator("risk_scores", mode="after")
    @classmethod
    def validate_scores(
        cls, value: dict[str, AmlRiskConfig]
    ) -> dict[str, AmlRiskConfig]:
        intervals = sorted(value.values(), key=lambda x: x.min_value)
        int_start = intervals[0].min_value
        int_end = intervals[-1].max_value
        if int_start != 0 or int_end != 1:
            raise ValueError(
                f"risk scores should cover interval [0; 1], got [{int_start}; {int_end}]"
            )

        for config in value.values():
            if config.min_value > config.max_value:
                raise ValueError(f"min > max in {config}")

        return value


class AmlSplitConfig(BaseModel):
    state: Literal["disabled", "enabled"]
    access_id: str
    access_key: str
    access_point: str
    flow: Literal["fast", "accurate", "advanced"]
    cryptos: Annotated[
        dict[OnErrorOmit[TronSymbol], AmlCryptoConfig], Field(min_length=1)
    ]


class ExternalDrain(BaseModel):
    regular_split: RegularSplitConfig
    aml_check: AmlSplitConfig
