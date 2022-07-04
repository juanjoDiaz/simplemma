"""Main module."""


import lzma
import logging
import pickle
import re

from functools import lru_cache
from pathlib import Path

try:
    from .rules import apply_rules
    from .tokenizer import simple_tokenizer
except ImportError:  # ModuleNotFoundError, Python >= 3.6
    pass


LOGGER = logging.getLogger(__name__)

LANGLIST = ['bg', 'ca', 'cs', 'cy', 'da', 'de', 'el', 'en', 'es', 'et', 'fa', 'fi', 'fr', 'ga', 'gd', 'gl', 'gv', 'hu', 'hy', 'id', 'it', 'ka', 'la', 'lb', 'lt', 'lv', 'mk', 'nb', 'nl', 'pl', 'pt', 'ro', 'ru', 'sk', 'sl', 'sv', 'tr', 'uk']

AFFIXLEN = 2
LONGAFFIXLEN = 5  # better for some languages
MINCOMPLEN = 4
#MAXLENGTH = 14

SAFE_LIMIT = {'cs', 'da', 'el', 'en', 'es', 'fi', 'fr', 'ga', 'hu', 'it', 'pl', 'pt', 'ru', 'sk', 'tr'}
BETTER_LOWER = {'bg', 'es', 'hy', 'lt', 'lv', 'pt', 'sk'}
BUFFER_HACK = {'bg', 'es', 'et', 'fi', 'fr', 'it', 'lt', 'pl', 'sk'}  # 'da'
LONGER_AFFIXES = {'et', 'fi', 'hu', 'hy', 'lt', 'ru'}  # 'pl'

HYPHEN_REGEX = re.compile(r'([_-])')
HYPHENS = {'-', '_'}
PUNCTUATION = {'.', '?', '!', '…', '¿', '¡'}

LANG_DATA = []

#class LangData:
#    "Class to store word pairs and relevant information."
#    __slots__ = ('dictionaries', 'languages')
#
#    def __init__(self):
#        self.languages = []
#        self.dictionaries = LangDict()


class LangDict:
    "Class to store word pairs and relevant information for a single language."
    __slots__ = ('code', 'dict')

    def __init__(self, langcode=None, langdict=None):
        self.code = langcode
        self.dict = langdict


def _determine_path(listpath, langcode):
    filename = f'{listpath}/{langcode}.txt'
    return str(Path(__file__).parent / filename)


def _load_dict(langcode, listpath='lists', silent=True):
    filepath = _determine_path(listpath, langcode)
    return _read_dict(filepath, langcode, silent)


def _read_dict(filepath, langcode, silent):
    mydict, myadditions, i = {}, [], 0
    leftlimit = 1 if langcode in SAFE_LIMIT else 2
    # load data from list
    with open(filepath , 'r', encoding='utf-8') as filehandle:
        for line in filehandle:
            columns = line.strip().split('\t')
            # invalid: remove noise
            # todo: exclude columns with punctuation!
            if len(columns) != 2 or len(columns[0]) < leftlimit or \
            line.startswith('-') or re.search(r'[+_]|[^ ]+ [^ ]+ [^ ]+', line) or \
            ':' in columns[1]:
                # or len(columns[1]) < 2:
                if silent is False:
                    LOGGER.warning('wrong format: %s', line.strip())
                continue
            # too long
            #if len(columns[0]) > MAXLENGTH:
            #    continue
            # process
            if columns[1] in mydict and mydict[columns[1]] != columns[0]:
                # prevent mistakes and noise coming from the lists
                dist1, dist2 = _levenshtein_dist(columns[1], mydict[columns[1]]), \
                    _levenshtein_dist(columns[1], columns[0])
                # fail-safe: delete potential false entry
                #if dist1 >= len(columns[1]) and dist2 >= len(columns[1]):
                #    del mydict[columns[1]]
                #    continue
                if dist1 == 0 or dist2 < dist1: # dist1 < 2
                    mydict[columns[1]] = columns[0]
                elif silent is False:
                    LOGGER.warning('diverging: %s %s | %s %s', columns[1], mydict[columns[1]], columns[1], columns[0])
                    LOGGER.debug('distances: %s %s', dist1, dist2)
            else:
                mydict[columns[1]] = columns[0]
                # deal with verbal forms (mostly)
                if langcode in BUFFER_HACK:
                    myadditions.append(columns[0])
                elif columns[0] not in mydict:
                    mydict[columns[0]] = columns[0]
                i += 1
    # overwrite
    for word in myadditions:
        mydict[word] = word
    LOGGER.debug('%s %s', langcode, i)
    return dict(sorted(mydict.items()))


def _pickle_dict(langcode):
    mydict = _load_dict(langcode)
    filename = f'data/{langcode}.plzma'
    filepath = str(Path(__file__).parent / filename)
    with lzma.open(filepath, 'wb') as filehandle: # , filters=my_filters, preset=9
        pickle.dump(mydict, filehandle, protocol=4)
    LOGGER.debug('%s %s', langcode, len(mydict))


def _load_pickle(langcode):
    filename = f'data/{langcode}.plzma'
    filepath = str(Path(__file__).parent / filename)
    with lzma.open(filepath, 'rb') as filehandle:
        return pickle.load(filehandle)


def _load_data(langs):
    """Decompress und unpickle lemmatization rules.
       Takes one or several ISO 639-1 code language code as input.
       Returns a list of dictionaries."""
    langlist = []
    for lang in langs:
        if lang not in LANGLIST:
            LOGGER.error('language not supported: %s', lang)
            continue
        LOGGER.debug('loading %s', lang)
        langlist.append(LangDict(lang, _load_pickle(lang)))
    return langlist


def _update_lang_data(lang):
    # convert string
    if isinstance(lang, str):
        lang = (lang,)
    if not isinstance(lang, tuple):
        raise TypeError('lang argument must be a two-letter language code')
    # load corresponding data
    global LANG_DATA
    if not LANG_DATA or tuple(l.code for l in LANG_DATA) != lang:
        LANG_DATA = _load_data(lang)
        lemmatize.cache_clear()
    return lang


@lru_cache(maxsize=65536)
def _levenshtein_dist(str1, str2):
    # inspired by this noticeably faster code:
    # https://gist.github.com/p-hash/9e0f9904ce7947c133308fbe48fe032b
    if str1 == str2:
        return 0
    if len(str1) > len(str2):
        str1, str2 = str2, str1
    r1 = list(range(len(str2) + 1))
    r2 = [0] * len(r1)
    i = 0
    for c1 in str1:
        r2[0] = i + 1
        j = 0
        for c2 in str2:
            if c1 == c2:
                r2[j+1] = r1[j]
            else:
                a1, a2, a3 = r2[j], r1[j], r1[j+1]
                if a1 > a2:
                    if a2 > a3:
                        r2[j+1] = 1 + a3
                    else:
                        r2[j+1] = 1 + a2
                else:
                    if a1 > a3:
                        r2[j+1] = 1 + a3
                    else:
                        r2[j+1] = 1 + a1
            j += 1
        aux = r1
        r1, r2 = r2, aux
        i += 1
    return r1[-1]


#def _define_greediness(langcode):
#    if langcode in ('bg', 'es', 'fr', 'ru', 'uk'):
#        return False
#    return True


def _simple_search(token, datadict, initial=False):
    # beginning of sentence, reverse case
    if initial is True:
        token = token.lower()
    candidate = datadict.get(token)
    if candidate is None:
        # try upper or lowercase
        if token[0].isupper():
            candidate = datadict.get(token.lower())
        else:
            candidate = datadict.get(token.capitalize())
    return candidate


def _greedy_search(candidate, datadict, steps=1, distance=5):
    i = 0
    while candidate in datadict and (
        len(datadict[candidate]) < len(candidate) and
        _levenshtein_dist(datadict[candidate], candidate) <= distance
        ):
        candidate = datadict[candidate]
        i += 1
        if i >= steps:
            break
    return candidate


def _decompose(token, datadict, affixlen=0):
    candidate, plan_b = None, None
    # this only makes sense for languages written from left to right
    # AFFIXLEN or MINCOMPLEN can spare time for some languages
    for count in range(1, len(token)-MINCOMPLEN+1):
        part1, part2 = token[:-count], token[-count:]
        # part1_aff = token[:-(count + affixlen)]
        lempart1 = _simple_search(part1, datadict)
        if lempart1 is not None:
            # maybe an affix? discard it
            if count <= affixlen:
                candidate = lempart1
                break
            # account for case before looking for second part
            if token[0].isupper():
                part2 = part2.capitalize()
            lempart2 = _simple_search(part2, datadict)
            if lempart2 is not None:
                #print('#', part1, part2, affixlen, count)
                # candidate must be shorter
                # try original case, then substitute
                if lempart2[0].isupper():
                    substitute = part2.lower()
                else:
                    substitute = part2.capitalize()
                # try other case
                newcandidate = _greedy_search(substitute, datadict)
                # shorten the second known part of the token
                if newcandidate and len(newcandidate) < len(part2):
                    candidate = part1 + newcandidate.lower()
                # backup: equal length or further candidates accepted
                if candidate is None:
                    # try without capitalizing
                    newcandidate = _simple_search(part2, datadict)
                    if newcandidate and len(newcandidate) <= len(part2):
                        candidate = part1 + newcandidate.lower()
                    # even greedier
                    # with capital letter?
                    elif len(lempart2) < len(part2) + affixlen:
                        plan_b = part1 + lempart2.lower()
                        #print(part1, part2, affixlen, count, newcandidate, planb)
                    #elif newcandidate and len(newcandidate) < len(part2) + affixlen:
                        #plan_b = part1 + newcandidate.lower()
                        #print(part1, part2, affixlen, count, newcandidate, planb)
                    #else:
                    #    print(part1, part2, affixlen, count, newcandidate)
                break
    return candidate, plan_b


def _dehyphen(token, datadict, greedy):
    if not '-' in token and not '_' in token:
        return None
    splitted = HYPHEN_REGEX.split(token)
    if len(splitted) > 1 and len(splitted[-1]) > 0:
        # try to find a word form without hyphen
        subcandidate = ''.join([t.lower() for t in splitted if t not in HYPHENS])
        if token[0].isupper():
            subcandidate = subcandidate.capitalize()
        if subcandidate in datadict:
            return datadict[subcandidate]
        # decompose
        subcandidate = _simple_search(splitted[-1], datadict)
        # search further
        if subcandidate is None and greedy is True:
            subcandidate = _affix_search(splitted[-1], datadict)
        # return
        if subcandidate is not None:
            splitted[-1] = subcandidate
            return ''.join(splitted)
    return None


def _affix_search(wordform, datadict, maxlen=AFFIXLEN):
    for length in range(maxlen, 1, -1):
        candidate, plan_b = _decompose(wordform, datadict, affixlen=length)
        if candidate is not None:
            break
    # exceptionally accept a longer solution
    if candidate is None and plan_b is not None:
        candidate = plan_b
    return candidate


def _suffix_search(token, datadict):
    lastcount = 0
    for count in range(MINCOMPLEN, len(token)-MINCOMPLEN+1):
        #print(token[-count:], token[:-count], lastpart)
        part = _simple_search(token[-count:].capitalize(), datadict)
        if part is not None and len(part) <= len(token[-count:]):
            lastpart, lastcount = part, count
    if lastcount > 0:
        return token[:-lastcount] + lastpart.lower()
    return None


def _return_lemma(token, datadict, greedy=True, lang=None, initial=False):
    # filters
    if token.isnumeric():
        return token
    # dictionary search
    candidate = _simple_search(token, datadict, initial=initial)
    # simple rules
    if candidate is None:
        candidate = apply_rules(token, lang)
    # decomposition
    if candidate is None: # and greedy is True
        candidate = _dehyphen(token, datadict, greedy)
    else:
        newcandidate = _dehyphen(candidate, datadict, greedy)
        if newcandidate is not None:
            candidate = newcandidate
    # stop here in some cases
    if len(token) <= 8 or greedy is False:
        return candidate
    # greedy subword decomposition: suffix/affix search
    if candidate is None:
        # greedier subword decomposition: suffix search with character in between
        if lang in LONGER_AFFIXES:
            maxlen = LONGAFFIXLEN
        else:
            maxlen = AFFIXLEN
        candidate = _affix_search(token, datadict, maxlen)
        # try something else
        if candidate is None:
            candidate = _suffix_search(token, datadict)
    # try further hops, not sure this is always a good idea
    else:
        candidate = _greedy_search(candidate, datadict)
    return candidate


def is_known(token, lang=None):
    """Tell if a token is present in one of the loaded dictionaries.
       Case-insensitive, whole word forms only. Returns True or False."""
    if not isinstance(token, str):
        raise TypeError(f'Wrong input type, expected string, got {type(token)}')
    _ = _update_lang_data(lang)
    for language in LANG_DATA:
        if _simple_search(token, language.dict) is not None:
            return True
    return False
    # suggestion:
    #return any(
    #    _simple_search(token, language.dict) is not None for language in langdata
    #)


@lru_cache(maxsize=1048576)
def lemmatize(token, lang=None, greedy=False, silent=True, initial=False):
    """Try to reduce a token to its lemma form according to the
       language list passed as input.
       Returns a string.
       Can raise ValueError by silent=False if no lemma has been found."""
    if not isinstance(token, str):
        raise TypeError(f'Wrong input type, expected string, got {type(token)}')
    lang = _update_lang_data(lang)
    # start
    for i, l in enumerate(LANG_DATA, start=1):
        # determine default greediness
        #if greedy is None:
        #    greedy = _define_greediness(language)
        # determine lemma
        candidate = _return_lemma(token, l.dict, greedy=greedy, lang=l.code, initial=initial)
        if candidate is not None:
            if i != 1:
                LOGGER.debug('%s found in %s', token, l.code)
            return candidate
    if silent is False:
        raise ValueError(f'Token not found: {token}')
    # try to simply lowercase
    if lang[0] in BETTER_LOWER:  # and len(token) < 10 ?
        return token.lower()
    return token


def text_lemmatizer(text, lang=None, greedy=False, silent=True):
    """Convenience function to lemmatize a text using a simple tokenizer.
       Returns a list of tokens and lemmata."""
    lemmata = []
    last = '.'  # beginning is initial
    for match in simple_tokenizer(text, iterate=True):
        # lemmatize, simple heuristic for sentence boundary
        lemmata.append(
            lemmatize(
                match[0], lang=lang, greedy=greedy, silent=silent,
                initial=last in PUNCTUATION
            )
        )
        last = match[0]
    return lemmata


def lemma_iterator(text, lang=None, greedy=False, silent=True):
    """Convenience function to lemmatize a text using a simple tokenizer.
       Returns a list of tokens and lemmata."""
    last = '.'  # beginning is initial
    for match in simple_tokenizer(text, iterate=True):
        # lemmatize
        initial = last in PUNCTUATION
        last = match[0]
        yield lemmatize(
            match[0], lang=lang, greedy=greedy, silent=silent, initial=initial
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    for listcode in LANGLIST:
        _pickle_dict(listcode)
