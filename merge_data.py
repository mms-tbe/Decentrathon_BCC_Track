from file_processor_utility import BCCFileOrganizer

def main():
    """
    Scans for data files and merges them into single files.
    """
    organizer = BCCFileOrganizer()
    organizer.scan_repository()
    organizer.merge_files_by_type()

if __name__ == "__main__":
    main()
