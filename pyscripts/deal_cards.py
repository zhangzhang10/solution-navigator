import itertools as it
import random

ranks = ['A', 'K', 'Q', 'J', '10', '9', '8', '7', '6', '5', '4', '3', '2']
suits = ['H', 'D', 'C', 'S']
cards = it.product(ranks, suits)

def shuffle(deck):
    deck = list(deck)
    random.shuffle(deck)
    return iter(tuple(deck))

def cut(deck, n):
    d1, d2 = it.tee(deck, 2)
    top = it.islice(d1, n)
    bottom = it.islice(d2, n, None)
    return it.chain(top, bottom)

def deal(deck, num_hands, hand_size):
    iters = [iter(deck)] * hand_size
    return tuple(zip(*(tuple(it.islice(itr, num_hands) for itr in iters))))


if __name__ == '__main__':
    cards = shuffle(cards)
    print(*deal(cards, num_hands=13, hand_size=5), sep='\n')
    print(len(tuple(cards)))

