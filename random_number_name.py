import random

def read_items_from_file(filename):
    try:
        with open(filename, "r") as file:
            items = [line.strip() for line in file]
        return items
    except FileNotFoundError:
        print(f"File '{filename}' not found.")
        return []

def main():
    filename_in = "/home/huyluongquang/lqhuy/Temp/Tools/src/resources/input/listMember.txt"
    filename_out = "/home/huyluongquang/lqhuy/Temp/Tools/src/resources/output/listMember.txt"
    items = read_items_from_file(filename_in)

    if not items:
        print("No items found in the file.")
        return

    print("List of items:")
    for index, item in enumerate(items, start=1):
        print(f"{index}. {item}")

    try:
        num_items_to_select = int(input("Enter the number of items to select: "))
        if num_items_to_select < 0 or num_items_to_select > len(items):
            print("Invalid number of items selected. Please choose a valid number.")
            return
    except ValueError:
        print("Invalid input. Please enter a valid number.")
        return

    selected_items = random.sample(items, num_items_to_select)
    for selected_item in selected_items:
        items.remove(selected_item)

    print("========================================================================================================")
    print("Congratulations to:")
    for index, selected_item in enumerate(selected_items, start=1):
        print(f"{index}. {selected_item}")

    with open(filename_out, "w") as file:
        for remaining_item in items:
            file.write(remaining_item + "\n")

    print(f"Remaining items have been written back to '{filename_out}'.")

if __name__ == "__main__":
    main()
