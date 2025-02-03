from SiteManager import SiteManager
from TransactionManager import TransactionManager
from IOManager import IOManager
import os
import sys

def main(tests_directory):
    site_manager = SiteManager()

    test_files = [f for f in os.listdir(tests_directory) if f.endswith(".txt")]

    for test_file in test_files:
        print("######################## TEST_FILE: {} Starts ##########################".format(test_file))
        io_manager = IOManager(os.path.join(tests_directory, test_file))
        transaction_manager = TransactionManager(site_manager, io_manager)

        try:
            # Process each instruction
            while True:
                instruction = io_manager.get_instruction()
                if not instruction:
                    break
                io_manager.process_instruction(instruction, transaction_manager, site_manager)
                io_manager.currentTime += 1  # Increment time for each instruction
        finally:
            io_manager.close()
            print("######################## TEST_FILE: {} Closed ##########################\n\n".format(test_file))


if __name__ == "__main__":
    input_path = sys.argv[1] if len(sys.argv) >= 2 else None

    # input_path para is NOT empty, scan all test cases under this path
    if input_path is None:
        print("No input path detected, using default test cases.")
        input_path = "./Tests"
    else:
        input_path = sys.argv[1]

    main(input_path)

