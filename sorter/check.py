import struct

prev = None
check = True
with open("../../external_sort_app\storage\output\sorted.bin", "rb") as f:
    while True:
        data = f.read(8)
        if not data:
            break
        num = struct.unpack("d",data)[0]

        if prev is not None and num < prev:
            print("SAI!")
            check = False
            break
        prev = num

if check:
    print("OK - SORT ĐÚNG")
