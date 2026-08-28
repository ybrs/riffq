import pyarrow as pa
from typing import (
    Iterable,
    Literal,
    NotRequired,
    Protocol,
    TypedDict,
    overload,
)

class ErrorResult(TypedDict):
    error: str
    sql_state: NotRequired[str]  # default XX000
    severity: NotRequired[Literal["ERROR", "FATAL", "PANIC"]]  # default ERROR


class TagResult(TypedDict):
    tag: str


type QueryCallbackResult = ErrorResult | TagResult | pa.RecordBatchReader


class QueryCallback(Protocol):

    @overload
    def __call__(self, tag: str, is_tag: Literal[True]) -> None:  # type: ignore
        ...

    @overload
    def __call__(
        self, error: tuple[str, str, str], is_error: Literal[True]
    ) -> None: ...

    @overload
    def __call__(self, result: pa.RecordBatchReader) -> None: ...

    @overload
    def __call__(self, result: QueryCallbackResult) -> None: ...

    @overload
    def __call__(self, result: Iterable[QueryCallbackResult]) -> None: ...

