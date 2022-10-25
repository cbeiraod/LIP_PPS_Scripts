from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html
import logging

import sqlite3
import pandas
import shutil

import plotly.express as px

import lip_pps_run_manager as RM

def script_main(run_directory: Path, csv_file: Path):
    script_logger = logging.getLogger('convert_sqlite')

    if not run_directory.parent.is_dir():
        script_logger.info("The run base directory should be an existing directory")
        return

    if not csv_file.is_file():
        script_logger.info("The csv file with the measurement data must exist")
        return

    with RM.RunManager(run_directory.resolve()) as Michael:
        Michael.create_run(raise_error=True)

        (Michael.path_directory/"data").mkdir()
        shutil.copyfile(csv_file, (Michael.path_directory/"data"/"measurements.csv"))

        with Michael.handle_task("convert_to_sqlite"):
            sqlite_file = Michael.path_directory/"data"/"measurements.sqlite"

            with sqlite3.connect(sqlite_file) as sqlite3_connection:
                measurements_df = pandas.read_csv(Michael.path_directory/"data"/"measurements.csv")

                measurements_df.to_sql('measurements',
                                        sqlite3_connection,
                                        index=False,
                                        if_exists='replace')

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Converts data taken with an oscilloscope into our data format')
    parser.add_argument('--dir',
        metavar = 'path',
        help = 'Path to the run directory.',
        required = True,
        dest = 'directory',
        type = str,
    )
    parser.add_argument('--data',
        metavar = 'path',
        help = 'Path to the csv file with the measurements.',
        required = True,
        dest = 'csv_file',
        type = str,
    )
    parser.add_argument(
        '-l',
        '--log-level',
        help = 'Set the logging level',
        choices = ["CRITICAL","ERROR","WARNING","INFO","DEBUG","NOTSET"],
        default = "ERROR",
        dest = 'log_level',
    )
    parser.add_argument(
        '--log-file',
        help = 'If set, the full log will be saved to a file (i.e. the log level is ignored)',
        action = 'store_true',
        dest = 'log_file',
    )

    args = parser.parse_args()

    if args.log_file:
        logging.basicConfig(filename='logging.log', filemode='w', encoding='utf-8', level=logging.NOTSET)
    else:
        if args.log_level == "CRITICAL":
            logging.basicConfig(level=50)
        elif args.log_level == "ERROR":
            logging.basicConfig(level=40)
        elif args.log_level == "WARNING":
            logging.basicConfig(level=30)
        elif args.log_level == "INFO":
            logging.basicConfig(level=20)
        elif args.log_level == "DEBUG":
            logging.basicConfig(level=10)
        elif args.log_level == "NOTSET":
            logging.basicConfig(level=0)

    script_main(Path(args.directory), Path(args.csv_file))
