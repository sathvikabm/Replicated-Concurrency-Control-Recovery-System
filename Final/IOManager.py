class IOManager:
    def __init__(self, file_path):
        self.currentTime = 0
        self.filePointer = open(file_path, "r")  # Open the file for reading

    def get_instruction(self):
        # Fetch the next instruction from the file
        return self.filePointer.readline().strip()

    def process_instruction(self, instruction, transaction_manager, site_manager):
        parts = instruction.strip().split("(")
        command = parts[0]
        args = parts[1].rstrip(")").split(",") if len(parts) > 1 else []

        if command == "begin":
            transaction_id = args[0]
            transaction_manager.begin(transaction_id, self.currentTime)
        elif command == "R":
            transaction_id = args[0]
            variable_id = args[1]
            transaction_manager.read(transaction_id, variable_id)
        elif command == "W":
            transaction_id, variable_id, value = args
            transaction_manager.write(transaction_id, variable_id, int(value))
        elif command == "end":
            transaction_id = args[0]
            transaction_manager.end(transaction_id)
        elif command == "dump":
            self.dump(site_manager)
        elif command == "fail":
            site_id = int(args[0])
            site_manager.fail(site_id, self.currentTime)
        elif command == "recover":
            site_id = int(args[0])
            site_manager.recover(site_id, self.currentTime, transaction_manager)
        # Add more conditions here for other commands like 'R', 'FAIL', 'RECOVER', etc.
        else:
            print(f"Unknown instruction: {instruction}")

    def dump(self, site_manager):
        # Print out the committed values of all variables at all sites
        for site in site_manager.sites:
            DMofSite = site.data_manager
            print("-------Site Info Dump-------")
            print("Site id is: ", site.id)
            for variable in DMofSite.committedVariables:
                print(
                    "{}.{} with value {}".format(
                        variable.variable_name, site.id, variable.value
                    )
                )
            print("----------------------------\n")

    def close(self):
        # Close the file when done
        self.filePointer.close()
