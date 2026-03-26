from db import setup_database
from phase1_parser import run as phase1
from phase2_parser import run as phase2

if __name__ == "__main__":
    setup_database()

    print("1 → Crawl URLs")
    print("2 → Extract Data (Live Pipeline)")

    choice = input("Enter choice: ")

    if choice == "1":
        phase1()
    elif choice == "2":
        phase2()