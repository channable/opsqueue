import logging
from opsqueue.consumer import ConsumerClient, Strategy

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


def capitalize_word(word: str) -> str:
    output = word.capitalize()
    # print(f"Capitalized word: {word} -> {output}")
    return output


client = ConsumerClient("localhost:3999", "file:///tmp/opsqueue/capitalize_text/")
client.run_each_op(capitalize_word, strategy=Strategy.Random())
