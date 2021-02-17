import multiprocessing

from balsam.site.launcher._serial_mode import main

if __name__ == "__main__":
    multiprocessing.set_start_method("fork", force=True)
    main()
