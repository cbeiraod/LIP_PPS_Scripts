from pathlib import Path # Pathlib documentation, very useful if unfamiliar:
                         #   https://docs.python.org/3/library/pathlib.html
from distutils.dir_util import copy_tree
import shutil
import dateutil.parser as dp

import plotly.express as px

import logging

import random
import numpy
import sqlite3
import pandas

import lip_pps_run_manager as RM

from math import sqrt

from tqdm import tqdm

# structures for parsing the binary file format

def InfiniiumUnitsToString(unit:int):
    if unit == 1:
        return "Volts"
    elif unit == 2:
        return "Seconds"
    elif unit == 3:
        return "Constant"
    elif unit == 4:
        return "Amps"
    elif unit == 5:
        return "dB"
    elif unit == 6:
        return "Hz"
    else:
        return "Unknown"

## File header format
file_header_dtype = numpy.dtype([('cookie', 'S2'),
                                 ('version', 'S2'),
                                 ('file_size', 'i4'),
                                 ('num_waveforms', 'i4')])

## Waveform header format
waveform_header_dtype = numpy.dtype([('header_size', 'i4'),
                                     ('waveform_type', 'i4'),
                                     ('num_waveform_buffers', 'i4'),
                                     ('num_points', 'i4'),
                                     ('count', 'i4'),
                                     ('x_display_range', 'f4'),
                                     ('x_display_origin', 'f8'),
                                     ('x_increment', 'f8'),
                                     ('x_origin', 'f8'),
                                     ('x_units', 'i4'),
                                     ('y_units', 'i4'),
                                     ('date_string', 'S16'),
                                     ('time_string', 'S16'),
                                     ('frame_string', 'S24'),
                                     ('waveform_string', 'S16'),
                                     ('time_tag', 'f8'),
                                     ('segment_index', 'u4')])

# Waveform data header format
waveform_data_header_dtype = numpy.dtype([('header_size', 'i4'),
                                          ('buffer_type', 'i2'),
                                          ('bytes_per_point', 'i2'),
                                          ('buffer_size', 'i4')])

def script_main(
        directory:Path,
        output_directory:Path,
        plot_waveforms:bool=False,
        save_buffers:bool=False,
        ):

    script_logger = logging.getLogger('convert_scope')

    if not directory.is_dir():
        script_logger.info("The input directory should be an existing directory")
        return

    John = RM.RunManager(output_directory.resolve())
    John.create_run(raise_error=True)

    if not plot_waveforms:
        numFiles = len(list(directory.glob('wav*.bin')))
        if numFiles < 10:
            plot_waveforms = True
        else:
            waveform_plot_list = random.sample(range(1, numFiles), 10)


    with John.handle_task("convert_scope_data") as Oliver:
        # Copied data location
        copied_data = (Oliver.task_path/'original_data_from_oscilloscope').resolve()

        # Copy and save original data
        script_logger.info("Copying original data to backup location")
        copy_tree(str(directory.resolve()), str(copied_data))

        #if (output_directory/'waveforms.sqlite').exists():
        #    script_logger.info("Deleting old database file")
        #    (output_directory/'waveforms.sqlite').unlink()

        with sqlite3.connect(John.path_directory/'waveforms.sqlite') as sqlite3_connection:
            waveforms_df = pandas.DataFrame()
            waveform_buffer_df = pandas.DataFrame()
            run_metadata_df = pandas.DataFrame()
            waveform_metadata_df = pandas.DataFrame()
            waveform_buffer_metadata_df = pandas.DataFrame()

            if_exists = 'replace' # What to do if the table already exists in the output sqlite

            # TODO: How to guarantee the order of the files?
            n_trigger = 0
            channel_map = {}
            for path in tqdm (copied_data.glob('wav*.bin'), desc="Converting Scope data..."): # Loop on binary waveform files
                script_logger.info("  Processing run {}".format(path.name))

                with path.open(mode='rb') as runFile: # Open the file
                    # Prepare empty pandas dataframes for the information from this file
                    file__waveforms_df = pandas.DataFrame()
                    file__waveform_buffer_df = pandas.DataFrame()
                    file__run_metadata_df = pandas.DataFrame()
                    file__waveform_metadata_df = pandas.DataFrame()
                    file__waveform_buffer_metadata_df = pandas.DataFrame()

                    data_ok = True
                    file_header_ok = True
                    waveform_header_ok = True
                    waveform_data_header_ok = True

                    # Read the file header
                    file_header = numpy.fromfile(runFile, dtype=file_header_dtype, count=1)

                    script_logger.debug("    Got the following file header:")
                    script_logger.debug("      - Cookie: {}".format(file_header['cookie'][0]))
                    script_logger.debug("      - Version: {}".format(file_header['version'][0]))
                    script_logger.debug("      - File Size: {} bytes".format(file_header['file_size'][0]))
                    script_logger.debug("      - Number of Waveforms: {}".format(file_header['num_waveforms'][0]))

                    if file_header['cookie'][0] != b'AG': # Skip files with incorrect format
                        file_header_ok = False
                        script_logger.error("The file {} is not in the Agilent Binary Data format, skipping it...".format(path.name))
                        continue

                    file__run_metadata_df = pandas.concat(
                                                          [
                                                            file__run_metadata_df,
                                                            pandas.DataFrame(
                                                                {
                                                                    "n_trigger": n_trigger,
                                                                    "file_name": path.name,
                                                                    "file_version": int(file_header['version'][0]),
                                                                    "file_size": int(file_header['file_size'][0]),
                                                                    "number_waveforms": int(file_header['num_waveforms'][0]),
                                                                },
                                                                index=[0]
                                                            ),
                                                          ],
                                                          #ignore_index=True
                                                         )
                    #print(file__run_metadata_df)
                    file__run_metadata_df.set_index(["n_trigger"], inplace=True)
                    #print(file__run_metadata_df)

                    for waveform_idx in numpy.arange(file_header['num_waveforms'][0]): # Loop on the waveforms in the file. TODO: Is it possible to have more than one waveform per channel?
                        # Read the waveform header
                        waveform_header = numpy.fromfile(runFile, dtype=waveform_header_dtype, count=1)

                        channel_string = bytes(waveform_header['waveform_string'][0]).decode('utf-8')
                        frame_string   = bytes(waveform_header[   'frame_string'][0]).decode('utf-8')
                        date_string    = bytes(waveform_header[    'date_string'][0]).decode('utf-8')
                        time_string    = bytes(waveform_header[    'time_string'][0]).decode('utf-8')

                        script_logger.info("    Parsing {}".format(channel_string))
                        script_logger.debug("      Got the Waveform header:")
                        script_logger.debug("        - Header Size: {}".format(waveform_header['header_size'][0], 'i4'))
                        script_logger.debug("        - Type: {}".format(waveform_header['waveform_type'][0], 'i4'))
                        script_logger.debug("        - Number Buffers: {}".format(waveform_header['num_waveform_buffers'][0], 'i4'))
                        script_logger.debug("        - Number of Points: {}".format(waveform_header['num_points'][0], 'i4'))
                        script_logger.debug("        - Count: {}".format(waveform_header['count'][0], 'i4'))
                        script_logger.debug("        - Range X Display: {}".format(waveform_header['x_display_range'][0], 'f4'))
                        script_logger.debug("        - Origin X Display: {}".format(waveform_header['x_display_origin'][0], 'f8'))
                        script_logger.debug("        - X Increment: {}".format(waveform_header['x_increment'][0], 'f8'))
                        script_logger.debug("        - X Origin: {}".format(waveform_header['x_origin'][0], 'f8'))
                        script_logger.debug("        - X Units: {}".format(waveform_header['x_units'][0], 'i4'))
                        script_logger.debug("        - Y Units: {}".format(waveform_header['y_units'][0], 'i4'))
                        script_logger.debug("        - Date: {}".format(waveform_header['date_string'][0], 'S16'))
                        script_logger.debug("        - Time: {}".format(waveform_header['time_string'][0], 'S16'))
                        script_logger.debug("        - Frame: {}".format(waveform_header['frame_string'][0], 'S24'))
                        script_logger.debug("        - Waveform Label: {}".format(waveform_header['waveform_string'][0], 'S16'))
                        script_logger.debug("        - Time Tag: {}".format(waveform_header['time_tag'][0], 'f8'))
                        script_logger.debug("        - Segment Index: {}".format(waveform_header['segment_index'][0], 'u4'))

                        if waveform_header['header_size'][0] != 140: # Check correct format for waveform header, if wrong we stop parsing waveforms and move to the next file
                            waveform_header_ok = False
                            script_logger.error("The waveform header has a length which is not 140. This is unexpected and requires fixing, skipping rest of file")
                            break

                        if channel_string in channel_map:
                            channel_idx = channel_map[channel_string]
                        else:
                            channel_idx = len(channel_map)
                            channel_map[channel_string] = channel_idx

                        file__waveform_metadata_df = pandas.concat(
                                                                    [
                                                                        file__waveform_metadata_df,
                                                                        pandas.DataFrame(
                                                                            {
                                                                                "channel_idx": channel_idx,
                                                                                "waveform_idx": waveform_idx,
                                                                                "n_trigger": n_trigger,
                                                                                "header_size": waveform_header['header_size'][0],
                                                                                "waveform_type": waveform_header['waveform_type'][0],
                                                                                "number_buffers": waveform_header['num_waveform_buffers'][0],
                                                                                "number_points": waveform_header['num_points'][0],
                                                                                "count": waveform_header['count'][0],
                                                                                "x_display_range": waveform_header['x_display_range'][0],
                                                                                "x_display_origin": waveform_header['x_display_origin'][0],
                                                                                "x_increment": waveform_header['x_increment'][0],
                                                                                "x_origin": waveform_header['x_origin'][0],
                                                                                "raw_x_units": waveform_header['x_units'][0],
                                                                                "raw_y_units": waveform_header['y_units'][0],
                                                                                "x_units": InfiniiumUnitsToString(waveform_header['x_units'][0]),
                                                                                "y_units": InfiniiumUnitsToString(waveform_header['y_units'][0]),
                                                                                "date": date_string,
                                                                                "time": time_string,
                                                                                "datetime": dp.parse(date_string + ' ' + time_string),
                                                                                "frame": frame_string,
                                                                                "channel": channel_string,
                                                                                "time_tag": waveform_header['time_tag'][0],
                                                                                "segment_index": waveform_header['segment_index'][0],
                                                                            },
                                                                            index=[0]
                                                                        ),
                                                                    ],
                                                                    #ignore_index=True
                                                                  )
                        #print(file__waveform_metadata_df)
                        file__waveform_metadata_df.set_index(["n_trigger", "channel_idx", "waveform_idx"], inplace=True)
                        #print(file__waveform_metadata_df)

                        del channel_string
                        del frame_string
                        del date_string
                        del time_string

                        if waveform_header['num_waveform_buffers'][0] > 1:
                            script_logger.critical("Please review the code that is merging the waveform buffers together, this has not been tested")

                        processed_points = 0
                        y_data = None
                        for buffer_idx in range(waveform_header['num_waveform_buffers'][0]): # Loop on the buffers for this waveform. TODO: Is it possible to have more than 1 per waveform?
                            # Read the waveform buffer header
                            buffer_header = numpy.fromfile(runFile, dtype=waveform_data_header_dtype, count=1)

                            script_logger.debug("      Got the Waveform Data header:")
                            script_logger.debug("        - Header Size: {}".format(buffer_header['header_size'][0]))
                            script_logger.debug("        - Buffer Type: {}".format(buffer_header['buffer_type'][0]))
                            script_logger.debug("        - Bytes Per Point: {}".format(buffer_header['bytes_per_point'][0]))
                            script_logger.debug("        - Buffer Size: {}".format(buffer_header['buffer_size'][0]))

                            if buffer_header['header_size'][0] != 12: # Check correct format for waveform data header, if wrong we stop parsing waveform buffers
                                waveform_data_header_ok = False
                                script_logger.error("The waveform buffer header has a length which is not 12. This is unexpected and requires fixing, skipping rest of file")
                                break

                            file__waveform_buffer_metadata_df = pandas.concat(
                                                                                [
                                                                                    file__waveform_buffer_metadata_df,
                                                                                    pandas.DataFrame(
                                                                                        {
                                                                                            "channel_idx": channel_idx,
                                                                                            "waveform_idx": waveform_idx,
                                                                                            "buffer_idx": buffer_idx,
                                                                                            "n_trigger": n_trigger,
                                                                                            "header_size": buffer_header['header_size'][0],
                                                                                            "buffer_type": buffer_header['buffer_type'][0],
                                                                                            "bytes_per_point": buffer_header['bytes_per_point'][0],
                                                                                            "buffer_size": buffer_header['buffer_size'][0],
                                                                                            "x_units": InfiniiumUnitsToString(waveform_header['x_units'][0]),
                                                                                            "y_units": InfiniiumUnitsToString(waveform_header['y_units'][0]),
                                                                                        },
                                                                                        index=[0]
                                                                                    ),
                                                                                ],
                                                                                #ignore_index=True
                                                                             )
                            #print(file__waveform_buffer_metadata_df)
                            file__waveform_buffer_metadata_df.set_index(["n_trigger", "channel_idx", "waveform_idx", "buffer_idx"], inplace=True)
                            #print(file__waveform_buffer_metadata_df)

                            buffer_type = buffer_header['buffer_type'][0]
                            bytes_per_point = buffer_header['bytes_per_point'][0]
                            buffer_size = buffer_header['buffer_size'][0]
                            buffer_points = int(buffer_size/bytes_per_point)
                            del buffer_header
                            del buffer_size

                            # Build the dtype to read the data from the buffer
                            if buffer_type in [1,2,3,4,5]: # Float type data
                                dtype_string = 'f{}'.format(bytes_per_point)
                            elif buffer_type == 6: # Unsigned type data
                                dtype_string = 'u{}'.format(bytes_per_point)
                            else: # Unknown type data, read as RAW data
                                dtype_string = 'V{}'.format(bytes_per_point)
                            del buffer_type
                            del bytes_per_point

                            channel_dtype = numpy.dtype([('data', dtype_string)])
                            del dtype_string

                            # Read the buffer data from the file
                            amplitude_data = numpy.fromfile(runFile, dtype=channel_dtype, count=buffer_points)
                            del channel_dtype

                            if len(amplitude_data) != buffer_points:
                                script_logger.error("There is a serious issue, asked to read {} points, but only {} were read. Maybe a corrupt file?".format(buffer_points, len(amplitude_data)))
                                script_logger.error("The file will be ignored")
                                data_ok = False
                                break

                            time_idx = numpy.arange(buffer_points)
                            time_data = time_idx * waveform_header['x_increment'][0] + waveform_header['x_origin'][0]
                            del buffer_points

                            file__waveform_buffer_df = pandas.concat(
                                                                        [
                                                                            file__waveform_buffer_df,
                                                                            pandas.DataFrame(
                                                                                {
                                                                                    "channel_idx": channel_idx,
                                                                                    "waveform_idx": waveform_idx,
                                                                                    "buffer_idx": buffer_idx,
                                                                                    "n_trigger": n_trigger,
                                                                                    "x": time_data,
                                                                                    "y": amplitude_data.astype(float),
                                                                                    "x_idx": time_idx,
                                                                                },
                                                                            ),
                                                                        ],
                                                                        #ignore_index=True
                                                                    )
                            #print(file__waveform_buffer_df)
                            file__waveform_buffer_df.set_index(["n_trigger", "channel_idx", "waveform_idx", "buffer_idx", "x_idx"], inplace=True)
                            #print(file__waveform_buffer_df)
                            processed_points += len(amplitude_data)

                            if y_data is None:
                                y_data = amplitude_data.astype(float)
                            else:
                                y_data = numpy.hstack([y_data, amplitude_data.astype(float)])
                            del amplitude_data
                        del buffer_idx

                        if not waveform_data_header_ok or not data_ok:
                            break

                        if processed_points != waveform_header['num_points'][0]:
                            script_logger.error("There is a mismatch between the number of points reported in the waveform header and the total number of points in the buffers. Skipping this file")
                            data_ok = False
                            break

                        time_idx = numpy.arange(waveform_header['num_points'][0])
                        time_data = time_idx * waveform_header['x_increment'][0] + waveform_header['x_origin'][0]

                        file__waveforms_df = pandas.concat(
                                                            [
                                                                file__waveforms_df,
                                                                pandas.DataFrame(
                                                                    {
                                                                        "channel_idx": channel_idx,
                                                                        "waveform_idx": waveform_idx,
                                                                        "n_trigger": n_trigger,
                                                                        "x": time_data,
                                                                        "y": y_data,
                                                                        "x_idx": time_idx,
                                                                    },
                                                                ),
                                                            ],
                                                            #ignore_index=True
                                                           )
                        #print(file__waveforms_df)
                        file__waveforms_df.set_index(["n_trigger", "channel_idx", "waveform_idx", "x_idx"], inplace=True)
                        #print(file__waveforms_df)

                        del time_idx
                        del time_data
                        del y_data
                        del processed_points
                        del channel_idx
                        del waveform_header
                    del waveform_idx

                    if file_header_ok and waveform_header_ok and waveform_data_header_ok and data_ok:
                        n_trigger += 1 # Only increment if the file was correctly processed

                        # Put data into the final data frames:
                        run_metadata_df = pandas.concat(
                                                        [
                                                            run_metadata_df,
                                                            file__run_metadata_df
                                                        ],
                                                        #ignore_index=True
                                                       )
                        waveform_metadata_df = pandas.concat(
                                                        [
                                                            waveform_metadata_df,
                                                            file__waveform_metadata_df
                                                        ],
                                                        #ignore_index=True
                                                       )
                        waveform_buffer_metadata_df = pandas.concat(
                                                        [
                                                            waveform_buffer_metadata_df,
                                                            file__waveform_buffer_metadata_df
                                                        ],
                                                        #ignore_index=True
                                                       )
                        waveforms_df = pandas.concat(
                                                        [
                                                            waveforms_df,
                                                            file__waveforms_df
                                                        ],
                                                        #ignore_index=True
                                                       )
                        waveform_buffer_df = pandas.concat(
                                                        [
                                                            waveform_buffer_df,
                                                            file__waveform_buffer_df
                                                        ],
                                                        #ignore_index=True
                                                       )

                        if plot_waveforms or n_trigger in waveform_plot_list:
                            plot_dir = Oliver.task_path/path.name
                            plot_dir.resolve()
                            plot_dir.mkdir(exist_ok=True)

                            fig = px.line(
                                file__waveforms_df.reset_index(["waveform_idx", "channel_idx"]),
		    	                x = 'x',
			                    y = 'y',
			                    facet_row = 'waveform_idx',
			                    line_group = 'channel_idx',
                                labels = {
                                    "x": "Time (s)",
                                    "y": "Amplitude (V)",
                                },
	    		                render_mode = 'webgl', # https://plotly.com/python/webgl-vs-svg/
                                title = "Waveform of {}".format(path.name)
                            )

                            fig.write_html(
	    		                str(plot_dir/'waveform.html'),
                                full_html = False, # For saving a html containing only a div with the plot
			                    include_plotlyjs = 'cdn',
		                    )

                            del fig

                        if len(waveforms_df.index) > 2e6:
                            script_logger.info('Saving run metadata into database...')
                            run_metadata_df.to_sql('run_metadata',
                                                   sqlite3_connection,
                                                   #index=False,
                                                   if_exists=if_exists)
                            run_metadata_df = pandas.DataFrame()

                            script_logger.info('Saving waveform metadata into database...')
                            waveform_metadata_df.to_sql('waveform_metadata',
                                                        sqlite3_connection,
                                                        #index=False,
                                                        if_exists=if_exists)
                            waveform_metadata_df = pandas.DataFrame()

                            script_logger.info('Saving waveform buffer metadata into database...')
                            waveform_buffer_metadata_df.to_sql('waveform_buffer_metadata',
                                                               sqlite3_connection,
                                                               #index=False,
                                                               if_exists=if_exists)
                            waveform_buffer_metadata_df = pandas.DataFrame()

                            if save_buffers:
                                script_logger.info('Saving waveform buffer into database...')
                                waveform_buffer_df.to_sql('waveform_buffer',
                                                          sqlite3_connection,
                                                          #index=False,
                                                          if_exists=if_exists)
                            waveform_buffer_df = pandas.DataFrame()

                            script_logger.info('Saving waveforms into database...')
                            waveforms_df.to_sql('waveforms',
                                                sqlite3_connection,
                                                #index=False,
                                                if_exists=if_exists)
                            waveforms_df = pandas.DataFrame()

                            if_exists = "append" # Since we already wrote some of the databases to the output file, now we want to append
                    del file__run_metadata_df
                    del file__waveform_metadata_df
                    del file__waveform_buffer_metadata_df
                    del file__waveforms_df
                    del file__waveform_buffer_df
                    del data_ok
                    del file_header_ok
                    del waveform_header_ok
                    del waveform_data_header_ok
                    del file_header
                del runFile

                script_logger.info("")
            del path

            # Write dataframes to database
            script_logger.info('Saving channel map into database...')
            channel_list = []
            idx_list = []
            for channel, idx in channel_map.items():
                channel_list += [channel]
                idx_list += [idx]
            channel_map_df = pandas.DataFrame(
                                {
                                    "channel_name": channel_list,
                                    "channel_idx": idx_list,
                                }
                             )
            channel_map_df.set_index(["channel_idx"], inplace=True)
            channel_map_df.to_sql('channel_map',
                                  sqlite3_connection,
                                  #index=False,
                                  if_exists='replace')
            del channel_map_df
            del channel_map
            del channel_list
            del idx_list
            del channel
            del idx

            script_logger.info('Saving run metadata into database...')
            run_metadata_df.to_sql('run_metadata',
                                   sqlite3_connection,
                                   #index=False,
                                   if_exists=if_exists)
            del run_metadata_df

            script_logger.info('Saving waveform metadata into database...')
            waveform_metadata_df.to_sql('waveform_metadata',
                                        sqlite3_connection,
                                        #index=False,
                                        if_exists=if_exists)
            del waveform_metadata_df

            script_logger.info('Saving waveform buffer metadata into database...')
            waveform_buffer_metadata_df.to_sql('waveform_buffer_metadata',
                                               sqlite3_connection,
                                               #index=False,
                                               if_exists=if_exists)
            del waveform_buffer_metadata_df

            if save_buffers:
                script_logger.info('Saving waveform buffer into database...')
                waveform_buffer_df.to_sql('waveform_buffer',
                                          sqlite3_connection,
                                          #index=False,
                                          if_exists=if_exists)
            del waveform_buffer_df

            script_logger.info('Saving waveforms into database...')
            waveforms_df.to_sql('waveforms',
                                sqlite3_connection,
                                #index=False,
                                if_exists=if_exists)
            del waveforms_df
            del if_exists
            del n_trigger
        del sqlite3_connection
        del plot_dir

        # Zip and delete the backed up data
        script_logger.info("Compressing the backup data")
        shutil.make_archive(str(copied_data), 'zip', str(copied_data))
        shutil.rmtree(copied_data)
        del copied_data
    del Oliver
    del waveform_plot_list

    script_logger.info('Finished converting to sqlite format...')

    with John.handle_task("average_waveform") as Mike:
        with sqlite3.connect(John.path_directory/'waveforms.sqlite') as sqlite3_connection:
            script_logger.info('Calculating average waveform...')

            # Make average plot (Processing with pandas; old approach)
            #waveforms_df = pandas.read_sql('SELECT * from waveforms', sqlite3_connection)
            #average_waveform_df = waveforms_df.groupby(["channel_idx", "waveform_idx", "x_idx"]).mean().drop(columns=["n_trigger"])

            # Make average plot (Processing with sqlite; new approach)
            average_waveform_df = pandas.read_sql(
                'SELECT waveform_idx, channel_idx, x_idx, AVG(x) AS x, AVG(y) AS y FROM waveforms GROUP BY x_idx, waveform_idx, channel_idx',
                sqlite3_connection).set_index(["channel_idx", "waveform_idx", "x_idx"])

            # Make dataframe of start times
            x_start_df = pandas.read_sql('SELECT * from waveforms WHERE x_idx=0', sqlite3_connection)
            x_start_var = x_start_df["x"].var()

            #print(x_start_df)
            #print(waveforms_df)
            #print(average_waveform_df)

            plot_dir = Mike.task_path#/"summary"
            plot_dir.resolve()
            plot_dir.mkdir(exist_ok=True)

            fig = px.histogram(
                x_start_df,
                x = 'x',
                labels = {
                    "x": "Start Time (s)",
                    "y": "Counts",
                },
                title = "Histogram of Waveform Start Times<br><sup>Standard Deviation: {}</sup>".format(sqrt(x_start_var))
            )

            fig.write_html(
                str(plot_dir/'waveform_start_times.html'),
                full_html = False, # For saving a html containing only a div with the plot
                include_plotlyjs = 'cdn',
            )

            fig = px.line(
                average_waveform_df.reset_index(["waveform_idx", "channel_idx"]),
                x = 'x',
                y = 'y',
                facet_row = 'waveform_idx',
                line_group = 'channel_idx',
                labels = {
                    "x": "Time (s)",
                    "y": "Amplitude (V)",
                },
                render_mode = 'webgl', # https://plotly.com/python/webgl-vs-svg/
                title = "Average Waveform"
            )

            fig.write_html(
                str(plot_dir/'average_waveform.html'),
                full_html = False, # For saving a html containing only a div with the plot
                include_plotlyjs = 'cdn',
            )

            script_logger.info('Saving average waveform into database...')
            average_waveform_df.to_sql('average_waveform',
                                       sqlite3_connection,
                                       #index=False,
                                       if_exists="replace")

        #script_logger.info("Compressing the sqlite data")
        #shutil.make_archive(str(output_directory/'waveforms.sqlite'), 'zip', str(output_directory), 'waveforms.sqlite')

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Converts data taken with an oscilloscope into our data format')
    parser.add_argument('--dir',
        metavar = 'path',
        help = 'Path to the base measurement directory.',
        required = True,
        dest = 'directory',
        type = str,
    )
    parser.add_argument(
        '-p',
        '--plot-waveforms',
        help = 'If this flag is passed, all the waveforms are plotted. Default is not plot, reason is that it takes some time and the resulting plots are heavy.',
        action = 'store_true',
        dest = 'plot_waveforms',
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
    parser.add_argument(
        '-b',
        '--save-buffers',
        help = 'If set, the individual waveform buffers will be saved to the output sqlite file',
        action = 'store_true',
        dest = 'save_buffers',
    )
    parser.add_argument(
        '-o',
        '--out-directory',
        metavar = 'path',
        help = 'Path to the output directory for the run data.',
        default = "./out",
        dest = 'out_directory',
        type = str,
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

    script_main(Path(args.directory), Path(args.out_directory), plot_waveforms=args.plot_waveforms, save_buffers=args.save_buffers)
