# set1 = [10, 10, 10, 2, 4, 0]

# for i in set1:
#     if i < 9:
#         set1.remove(i)
# print(set1)


def removeElements(lst, k):
    return [i for i in lst if i > k]


lst = [10, 10, 10, 2, 4, 0]
lst2 = removeElements(lst, 9)
print(lst2)