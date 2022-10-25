from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html
import logging

import sqlite3
import pandas
import shutil

import plotly.express as px

import lip_pps_run_manager as RM

def script_main(run_directory: Path, device_name: str, reference_curves = []):
    script_logger = logging.getLogger('plot_IV_curve')

    with RM.RunManager(run_directory.resolve()) as Michael:
        Michael.create_run(raise_error=False)

        if not Michael.task_completed("convert_to_sqlite"):
            script_logger.error("You must first convert the IV data from CSV to SQLITE")
            return

        with Michael.handle_task("plot_IV_curve") as Maria:
            sqlite_file = Michael.path_directory/"data"/"measurements.sqlite"

            with sqlite3.connect(sqlite_file) as sqlite3_connection:
                measurements_df = pandas.read_sql('SELECT * FROM measurements', sqlite3_connection, index_col=None)
                measurements_df["measurement"] = "LIP"

                for curve in reference_curves:
                    name = curve["name"]
                    location = curve["location"]
                    data_type = curve["type"]
                    invert = curve["invert"]

                    if not location.is_file():
                        continue

                    if data_type == "feather":
                        reference_curve_df = pandas.read_feather(location)
                    else:
                        continue

                    reduced_df = reference_curve_df[["Bias voltage (V)", "Bias current (A)"]].copy()

                    if invert:
                        reduced_df["Bias voltage (V)"] = -reduced_df["Bias voltage (V)"]
                        reduced_df["Bias current (A)"] = -reduced_df["Bias current (A)"]
                        pass
                    reduced_df["measurement"] = name

                    #measurements_df = measurements_df.append(reduced_df, ignore_index=True)
                    measurements_df = pandas.concat([measurements_df, reduced_df], axis=0, ignore_index=True)

                color_column = None
                if len(reference_curves) > 0:
                    color_column = "measurement"

                fig = px.line(
        			data_frame = measurements_df,
        			x = 'Bias voltage (V)',
		        	y = 'Bias current (A)',
                    color=color_column,
        			title = 'IV curve<br><sup>Measurement: {}</sup>'.format(device_name),
		        	markers = '.',
		        )
                fig.write_html(Maria.task_path/"IV_curve.html", include_plotlyjs='cdn')

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
    parser.add_argument('-d', '--device',
        help = 'The device name.',
        required = True,
        dest = 'device',
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

    reference_curves = [
        {
            "name": "20º C (Zurich Reference)",
            "location": Path("/Users/cristovao/CERNBOX_LGAD/Zurich-Reference/Sample27-Positive20C/IV_curve/measured_data.fd"),
            "type": "feather",
            "invert": True,
        },
        {
            "name": "-20º C (Zurich Reference)",
            "location": Path("/Users/cristovao/CERNBOX_LGAD/Zurich-Reference/Sample27-Negative20C/IV_curve/measured_data.fd"),
            "type": "feather",
            "invert": True,
        },
    ]

    script_main(Path(args.directory), args.device, reference_curves)