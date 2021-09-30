"""
Map Reduce Assignment - Jose Rodriguez

Instructions + full documentation of this assignment are found inside of the
readme file.

You can get the help menus of this program by passing in the --help flag
"""

import argparse
import pymp
import time


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


def clean_word(word):
    """
    Removes any weird punctuation inside of a word
    """
    return ''.join(c for c in word if c.isalpha()).lower()


def count_words_from_file(filename):
    """
    Given a filename, will open the filename and counts the words that are
    being searched for.
    """
    out = {word: 0 for word in words_to_count}

    with open(filename, 'r') as opened_file:
        for line in opened_file:
            line = line.lower()
            for word in words_to_count:
                out[word] += line.count(word)

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
            # dictionry
            sum_lock.acquire()
            add_dictionaries(current_counts, shared_dict)
            sum_lock.release()

    return dict(shared_dict)


def main():
    parser = argparse.ArgumentParser(
        description="Map Reduce assignment. Counts words inside of Shakespeare's works!")
    parser.add_argument('--silent', default=False, type=bool,
                        help="If set to true, will only print timing results in addition to the number of threads")
    parser.add_argument('--num_threads', default=1, type=int,
                        help="Number of  threads which will be created in order to count the number of words.")
    parser.add_argument('--no_pymp', default=False, type=bool,
                        help="If set to true, a synchronous version that does not use pymp will run.")
    args = parser.parse_args()

    start = time.clock_gettime(time.CLOCK_MONOTONIC)
    if args.no_pymp:
        counts = compute_synchronously()
    else:
        counts = compute_with_map_reduce(args.num_threads)
    time_taken = time.clock_gettime(time.CLOCK_MONOTONIC) - start

    print(
        f"(pymp: {not args.no_pymp}) (# threads: {args.num_threads}) Time taken: {time_taken}")
    if not args.silent:
        print(f"Counts: {counts}")


if __name__ == '__main__':
    main()
