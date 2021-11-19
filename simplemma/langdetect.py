"""Experimental language detection."""

import re

from collections import Counter
from operator import itemgetter

from .simplemma import _simple_search, _return_lemma  # load_data, LANGLIST


SPLIT_INPUT = re.compile(r'[^\W\d_]{3,}')


def prepare_text(text):
    """Extract potential words, scramble them, extract the most frequent,
       some of the rest, and return at most 1000 tokens."""
    # generator expression to split the text
    counter = Counter(match[0] for match in SPLIT_INPUT.finditer(text))
    #total = sum(counter.values())
    #if total > 100:
    #    # take about 10% of the tokens
    #    limit = int(sum(counter.values())/10)
    #else:
    #    limit = total
    #most_frequent_short = [item[0] for item in counter.most_common(10)]
    #rest = [t for t in set(tokens) if len(t) > 4 and t not in most_frequent][:990]
    return [item[0] for item in counter.most_common(1000)]


def in_target_language(text, langdata):
    """Determine which proportion of the text is in the target language."""
    total = 0
    in_target = 0
    for token in prepare_text(text):
        total += 1
        for language in langdata:
            candidate = _return_lemma(token, language[1], greedy=True, lang=language[0])
            if candidate is not None:
                in_target += 1
    return in_target/total


def lang_detector(text, langdata, extensive=False):
    """Determine which proportion of the text is in the target language(s)."""
    myresults = {}
    found = set()
    tokens = prepare_text(text)
    for langcode, langdict in langdata:
        in_target = []
        for token in tokens:
            if extensive is False:
                result = _simple_search(token, langdict)
            else:
                result = _return_lemma(token, langdict, greedy=True, lang=langcode)
            if result is not None:
                in_target.append(token)
        found.update(in_target)
        myresults[langcode] = len(in_target)/len(tokens)
    myresults['unk'] = (len(tokens)-len(found))/len(tokens)
    results = sorted(myresults.items(), key=itemgetter(1), reverse=True)
    # in case of ex-aequo
    if extensive is False and results[0][1] == results[1][1]:
        results = lang_detector(text, langdata, extensive=True)
    # todo: None if 'unk'?
    return results
