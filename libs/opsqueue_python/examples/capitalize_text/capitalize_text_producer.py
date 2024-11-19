import logging
import re
from opsqueue.producer import ProducerClient
from collections.abc import Iterable

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)

client = ProducerClient("localhost:3999", "file:///tmp/opsqueue/capitalize_text/")


def file_to_words(filename: str) -> Iterable[str]:
    """
    Iterates over each word and inter-word whitespace strings in a file
    while keeping at most one line in memory at a time.
    """
    with open(filename) as input_file:
        for line in input_file:
            for word in re.split("\\b", line):
                yield word


def print_words(words: Iterable[str]) -> None:
    """
    Prints all words and inter-word whitespace tokens
    without first loading the full string into memory
    """
    for word in words:
        print(word, end="")


# words_iter = file_to_words("lipsum.txt")
words_iter = file_to_words("more_lipsum.txt")

capitalized_words = client.run_submission(words_iter, chunk_size=4000)

print_words(capitalized_words)
