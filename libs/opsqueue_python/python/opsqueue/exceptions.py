## Expected errors:

from . import opsqueue_internal


class SubmissionFailedError(Exception):
    __slots__ = ["submission", "chunk", "failure"]
    """
    Raised when a submission failed,

    that is: One of the chunks of this submission
    failed more than `retry_count` times,
    (default: 10, configurable when Opsqueue is started).

    This means that part of the submission is missing and therefore
    it can never be 'completed' anymore.
    """

    def __init__(
        self,
        submission: opsqueue_internal.SubmissionFailed,
        chunk: opsqueue_internal.ChunkFailed,
    ):
        super().__init__()
        self.submission = submission
        self.chunk = chunk
        self.failure = self.chunk.failure

    def __str__(self) -> str:
        return f"""
        Submission {self.submission.id} failed because chunk {self.chunk.chunk_index} kept failing.
        Failure reason:

        {self.failure}
        """

    def __repr__(self) -> str:
        return str(self)


## Usage errors:


class IncorrectUsageError(TypeError):
    """
    Base exception class for all different ways
    Python code might be calling the Opsqueue's client library
    incorrectly.
    """

    pass


class TryFromIntError(IncorrectUsageError):
    """
    Raised when code expects a particular subset of integers,
    but an invalid integer is passed (such as passing a negative value
    to a place expecting zero or positive).
    """

    pass


class ChunkNotFoundError(IncorrectUsageError):
    """
    Raised when a method is used to look up information about a chunk
    but the chunk doesn't exist within the Opsqueue.
    """

    pass


class SubmissionNotFoundError(IncorrectUsageError):
    """
    Raised when a method is used to look up information about a submission
    but the submission doesn't exist within the Opsqueue.
    """

    pass


class ChunkCountIsZeroError(IncorrectUsageError):
    """
    Raised when making an empty submission.

    TODO it is likely that empty submissions should not be a user error
    but rather be handled transparently by Opsqueue.
    """

    pass


class NewObjectStoreClientError(IncorrectUsageError):
    """
    Raised when incorrect settings were used when attempting to initialize
    the object store connection.
    """

    pass


class SubmissionNotCompletedYetError(IncorrectUsageError):
    """
    Raised when a method attempts to look up info
    from a completed submission but the submission
    is not completed yet.
    """

    pass


# Internal errors:


class OpsqueueInternalError(Exception):
    """
    Base type for all internal exceptions
    """

    pass


class UnexpectedOpsqueueConsumerServerResponseError(OpsqueueInternalError):
    """
    Handled when the consumer client receives as response
    to a sync method something other than what it had expected.

    This may indicate that the client and server versions are not matching,
    or that there is an implementatoin error in the Opsqueue code.
    """

    pass


class ChunkRetrievalError(OpsqueueInternalError):
    """
    Raised when there is a problem when fetching a chunk from the object store.
    """

    pass


class ChunksStorageError(OpsqueueInternalError):
    """
    Raised when there is a problem when the producer
    is attempting to store all its chunks into the object store.
    """

    pass


class ChunkStorageError(OpsqueueInternalError):
    """
    Raised when there is a problem when a consumer
    is storing a chunk to the object store.
    """

    pass


class InternalConsumerClientError(OpsqueueInternalError):
    """
    Raised for any othre kind of Consumer client exception
    """

    pass


class InternalProducerClientError(OpsqueueInternalError):
    """
    Raised for any othre kind of Producer client exception
    """

    pass
