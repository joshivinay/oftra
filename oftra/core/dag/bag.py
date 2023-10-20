# class Bag:
#     class Node:
#         def __init__(self, item):
#             self.item = item
#             self.next = None

#     class ListIterator:
#         def __init__(self, first):
#             self.current = first

#         def __iter__(self):
#             return self

#         def __next__(self):
#             if self.current is None:
#                 raise StopIteration
#             item = self.current.item
#             self.current = self.current.next
#             return item

#     def __init__(self):
#         self.N = 0  # number of elements in bag
#         self.first = None  # beginning of bag

#     def is_empty(self):
#         return self.first is None

#     def size(self):
#         return self.N

#     def add(self, item):
#         old_first = self.first
#         self.first = Bag.Node(item)
#         self.first.next = old_first
#         self.N += 1

#     def __iter__(self):
#         return Bag.ListIterator(self.first)

#     def __str__(self):
#         items = [item for item in self]
#         return "Bag(" + ", ".join(map(str, items)) + ")"

# if __name__ == "__main__":
#     bag = Bag()
#     bag.add("Item 1")
#     bag.add("Item 2")
#     bag.add("Item 3")

#     print("Is bag empty?", bag.is_empty())
#     print("Size of bag:", bag.size())
#     print("Bag contents:", bag)

#     for item in bag:
#         print("Item:", item)
