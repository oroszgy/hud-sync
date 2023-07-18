import re

from conllu import Token, SentenceList

PLUS_IN_LEMMA: re.Pattern = re.compile(r"(\w+)\+(\w+)")


def clean_lemma(token: Token):
    token["lemma"] = PLUS_IN_LEMMA.sub(r"\1\2", token["lemma"])


def clean_lemmata(data: SentenceList):
    for sent in data:
        for tok in sent:
            clean_lemma(tok)
