import re

from conllu import Token, SentenceList


PLUS_IN_LEMMA: re.Pattern = re.compile(r"(\w+)\+(\w+)")
NUMBERS: re.Pattern = re.compile("\d+")
ROMAN_NUMBERS: re.Pattern = re.compile(r"^(M{0,3}(CM|CD|D?C{0,3})?(XC|XL|L?X{0,3})?(IX|IV|V?I{0,3})?)(?P<suffix>(\.)|(-\w+)?)$")

def _normalize_token(token: Token):
    orig_form = token["form"]
    orig_lemma = token["lemma"]

    form_wo_numbers = NUMBERS.sub("<NUM>", orig_form)
    roman_match: re.Match = ROMAN_NUMBERS.match(orig_form)
    if roman_match:
        token["form"] = ROMAN_NUMBERS.sub(r"<ROMAN>\g<suffix>", orig_form)
        token["lemma"] = ROMAN_NUMBERS.sub(r"<ROMAN>\g<suffix>", orig_lemma)
        token["lemma"] = NUMBERS.sub("<NUM>", token["lemma"])
    elif form_wo_numbers != orig_form:
        token["form"] = form_wo_numbers
        token["lemma"] = NUMBERS.sub("<NUM>", orig_lemma)
    else:
        token["lemma"] = PLUS_IN_LEMMA.sub(r"\1\2", orig_lemma)


def normalize(data: SentenceList):
    for sent in data:
        for tok in sent:
            _normalize_token(tok)
