import itertools
import json
import lzma
from operator import itemgetter
from os import mkdir
from pathlib import Path
import pickle
import re
from typing import ByteString, Dict, Iterator
import urllib.request

from simplemma.strategies.defaultrules import DEFAULT_RULES
from simplemma.strategies.dictionaries.dictionary_factory import SUPPORTED_LANGUAGES
from simplemma.utils import levenshtein_dist

INPUT_PUNCT = re.compile(r"[,:*/\+_]|^-|-\t")
SAFE_LIMIT = {
    "cs",
    "da",
    "el",
    "en",
    "es",
    "fi",
    "fr",
    "ga",
    "hu",
    "it",
    "pl",
    "pt",
    "ru",
    "sk",
    "tr",
}

VOC_LIMIT = {"fi", "la", "pl", "pt", "sk", "tr"}

MAXLENGTH = 16

DATA_FOLDER = Path(__file__) / "dictionaries_data"


def get_lemmas_from_kaikki() -> Iterator[(str, str)]:
    target_url = 'https://kaikki.org/dictionary/raw-wiktextract-data.json'
    for line in urllib.request.urlopen(target_url):
        item = json.loads(line.decode('utf-8'))
        lang_code = item['lang_code']
        if lang_code not in SUPPORTED_LANGUAGES:
            continue

        if 'senses' in item:
            for s in item['senses']:
                if 'form_of' in s:
                    yield (lang_code, s['form_of'][0]['word'], item['word'])
                elif 'alt_of' in s:
                    yield (lang_code, s['alt_of'][0]['word'], item['word'])
                continue
        
        if 'forms' in item:
            for f in item['forms']:
                yield (lang_code, item['word'], f['form'])


def filter_lemmas(lemmas: Iterator[(str, str, str)]) -> Iterator[(str, str)]:
    leftlimit = 1 if lang_code in SAFE_LIMIT else 2
    for lang_code, lemma, form in lemmas:
        if not form:
            continue

        if INPUT_PUNCT.search(lemma) or INPUT_PUNCT.search(form):
                continue

        # invalid: remove noise
        if len(lemma) < leftlimit:
            # or len(form) < 2:
            # if not silent:
            #     LOGGER.warning("wrong format: %s", line.strip())
            continue
        # too long
        if lang_code in VOC_LIMIT and (
            len(lemma) > MAXLENGTH or len(form) > MAXLENGTH
        ):
            continue
        # length difference
        if len(lemma) == 1 and len(form) > 6:
            continue
        if len(lemma) > 6 and len(form) == 1:
            continue
        # Removed words that would be detected by rules
        if (
            len(form) > 6 and lang_code in DEFAULT_RULES
        ):  # form != lemma
            rule = DEFAULT_RULES[lang_code](form)
            if rule == lemma:
                continue
        
        yield lang_code, lemma, form


def get_lemmatization_dictionaries(lemmas: Iterator[(str, str, str)]) -> Dict[str, Dict[ByteString, ByteString]]:
    lemmas_per_lang = itertools.tee(lemmas, SUPPORTED_LANGUAGES)
    return {
        SUPPORTED_LANGUAGES[i]: get_ditionary_from_lemmas(
            (form, lemma)
            for lang_code, lemma, form in lemmas_per_lang[i]
            if lang_code == SUPPORTED_LANGUAGES[i]
        )
        for i in range(0, len(SUPPORTED_LANGUAGES))
    }


def get_ditionary_from_lemmas(lemmas: iter[(str, str)]) -> Dict[ByteString, ByteString]:
    dictionary = {}
    for form, lemma in lemmas:
        if form not in dictionary:
            dictionary[form] = lemma
        else:
            if dictionary[form] == lemma:
                continue

            # prevent mistakes and noise coming from the lists
            dist1 = levenshtein_dist(form, dictionary[form])
            dist2 = levenshtein_dist(form, lemma)
            if dist1 == 0 or dist2 < dist1:
                dictionary[form] = lemma
    return dictionary


def write_pickled_dictionary_to_file(langcode: str, dictionary: dict[str, str]) -> None:
    # sort dictionary to help saving space during compression
    # if langcode not in ("lt", "sw"):
    dictionary = dict(sorted(dictionary.items(), key=itemgetter(1)))
    filepath = str(DATA_FOLDER / f"{langcode}.plzma")

    with lzma.open(filepath, "wb") as filehandle:  # , filters=my_filters, preset=9
        pickle.dump(dictionary, filehandle, protocol=4)


def main():
    mkdir(DATA_FOLDER)
    lemmas = get_lemmas_from_kaikki()
    filtered_lemmas = lemmas # filter_lemmas(lemmas)
    lemmas_by_language = get_lemmatization_dictionaries(filtered_lemmas)
    for lang_code, dictionary in lemmas_by_language.items():
        write_pickled_dictionary_to_file(lang_code, dictionary)
