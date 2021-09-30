"""
Map Reduce Assignment - Jose Rodriguez

Instructions + full documentation of this assignment are found inside of the
readme file.

You can get the help menus of this program by passing in the --help flag
"""

import argparse
import pymp
import time

expected_dictionary = {
    "hate": 332,
    "love": 3070,
    "death": 1016,
    "night": 1402,
    "sleep": 470,
    "time": 1806,
    "henry": 661,
    "hamlet": 475,
    "you": 23306,
    "my": 14203,
    "blood": 1009,
    "poison": 139,
    "macbeth": 288,
    "king": 4545,
    "heart": 1458,
    "honest": 434,
}

files_to_search = [
    'shakespeare1.txt',
    'shakespeare2.txt',
    'shakespeare3.txt',
    'shakespeare4.txt',
    'shakespeare5.txt',
    'shakespeare6.txt',
    'shakespeare7.txt',
    'shakespeare8.txt',
]


words_to_count = [
    'hate',
    'love',
    'death',
    'night',
    'sleep',
    'time',
    'henry',
    'hamlet',
    'you',
    'my',
    'blood',
    'poison',
    'macbeth',
    'king',
    'heart',
    'honest',
]

TIME_FILE_READING = False
TIME_WORD_COUNTING = False


def clean_word(word):
    """
    Removes any weird punctuation inside of a word
    """
    return ''.join(c for c in word if c.isalpha()).lower()


def gettime():
    return time.clock_gettime(time.CLOCK_MONOTONIC)


def count_words_from_file(filename):
    """
    Given a filename, will open the filename and counts the words that are
    being searched for.
    """
    out = {word: 0 for word in words_to_count}

    start = gettime()
    with open(filename, 'r') as opened_file:
        contents = opened_file.read().lower()
        time_to_read = gettime() - start

        start = gettime()
        for word in words_to_count:
            out[word] += contents.count(word)
        time_to_count = gettime() - start

    if TIME_FILE_READING:
        print(f'{filename} time to read file: {time_to_read}')

    if TIME_WORD_COUNTING:
        print(f'{filename} time to count words: {time_to_count}')

    return out


def add_dictionaries(source, destination):
    """
    Adds a two dictionaries together by modifying the destination dictionary
    in-place.
    """
    for word in source:
        if word not in destination:
            destination[word] = 0
        destination[word] += source[word]


def compute_synchronously():
    out = {}
    for filename in files_to_search:
        add_dictionaries(count_words_from_file(filename), out)
    return out


def compute_with_map_reduce(num_threads):
    shared_dict = pymp.shared.dict()

    with pymp.Parallel(num_threads) as p:
        sum_lock = p.lock
        for filename in p.iterate(files_to_search):
            # Mapping the filename from a string into a dictionary count
            current_counts = count_words_from_file(filename)

            # Reducing the multiple dictionaries into a single aggregate
            # dictionary
            sum_lock.acquire()
            add_dictionaries(current_counts, shared_dict)
            sum_lock.release()

    return dict(shared_dict)


def verify(results):
    for key in results:
        if key not in expected_dictionary or results[key] != expected_dictionary[key]:
            assert False, 'Results did not match expected results'

    for key in expected_dictionary:
        if key not in results or results[key] != expected_dictionary[key]:
            assert False, 'Results did not match expected results'


def main():
    global TIME_FILE_READING
    global TIME_WORD_COUNTING

    parser = argparse.ArgumentParser(
        description="Map Reduce assignment. Counts words inside of Shakespeare's works!")
    parser.add_argument('--silent', default=False, type=bool,
                        help="If set to true, will only print timing results in addition to the number of threads")
    parser.add_argument('--num_threads', default=1, type=int,
                        help="Number of  threads which will be created in order to count the number of words.")
    parser.add_argument('--no_pymp', default=False, type=bool,
                        help="If set to true, a synchronous version that does not use pymp will run.")
    parser.add_argument('--time_file_reading', default=False, type=bool,
                        help="If set to true, this will time the file reading operation")
    parser.add_argument('--time_counting', default=False, type=bool,
                        help="If set to true, this will time the counting operations")
    args = parser.parse_args()

    TIME_FILE_READING = args.time_file_reading
    TIME_WORD_COUNTING = args.time_counting

    start = gettime()
    if args.no_pymp:
        counts = compute_synchronously()
    else:
        counts = compute_with_map_reduce(args.num_threads)
    time_taken = gettime() - start

    print(
        f"(pymp: {not args.no_pymp}) (# threads: {args.num_threads}) Time taken: {time_taken}")
    if not args.silent:
        print(f"Counts: {counts}")

    verify(counts)


if __name__ == '__main__':
    main()
