#!/usr/bin/env python3

import click
import sys,subprocess
import gpxpy
from pathlib import Path
from datetime import datetime, timedelta,timezone
import av
import exiftool

def extract_gps(insv_path : Path, gpx_path : Path) -> int:
    if gpx_path.exists():
        print("GPX file already exists, skipping extraction")
        return 0

    print("Extracting GPX")

    outfile = open(gpx_path, "w", encoding="utf-8")
    cmd = [
        "exiftool",
        "-api", "largefilesupport=1",
        "-p", Path(__file__).parent/"gpx.fmt",
        "-ee3",
        insv_path
    ]

    result = subprocess.run(cmd, stdout=outfile, stderr=sys.stderr)
    outfile.close()
    if result.returncode == 0:
        print("GPX extraction successful")
    
    return result.returncode

def get_start_time(insv_path : Path) -> datetime:
    cmd = [
        "exiftool",
        "-api", "largefilesupport=1",
        "-s", "-s", "-s",
        "-CreateDate",
        "-MediaCreateDate",
        "-TrackCreateDate",
        "-DateTimeOriginal",
        insv_path
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    ts = result.stdout.strip()
    if not ts:
        return None
    
    start_time = datetime.strptime(ts.partition('\n')[0], "%Y:%m:%d %H:%M:%S") # EXIF format: YYYY:MM:DD HH:MM:SS
    if start_time.tzinfo is None:
        print("Warning: start time has no time zone, assuming UTC.")
        start_time = start_time.replace(tzinfo=timezone.utc)
    return start_time

# def get_duration(insv_path : Path) -> timedelta:
#     cmd = [
#             "ffprobe", "-v", "error",
#             "-select_streams", "v:0",
#             "-show_entries", "stream=duration",
#             "-of", "csv=p=0",
#             insv_path
#         ]
#     out = subprocess.check_output(cmd, text=True).strip()
#     return timedelta(seconds=float(out)) if out else None

# def get_frames_and_pts(path):
#     cmd = [
#         "ffprobe",
#         "-v", "error",
#         "-select_streams", "v:0",
#         "-show_packets",
#         "-show_entries", "packet=pts_time",
#         "-of", "csv=p=0",
#         path
#     ]

#     proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

#     for frame_number, line in enumerate(proc.stdout):
#         ts = line.strip()
#         if ts:
#             yield frame_number, timedelta(seconds=float(ts))

#     proc.wait()



# def lerp_points(point_a : gpxpy.mod_gpx.GPXTrackPoint, point_b : gpxpy.mod_gpx.GPXTrackPoint, time:datetime) -> gpxpy.mod_gpx.GPXTrackPoint:
#     time_a = point_a.time
#     time_b = point_b.time
#     diff = time_b - time_a
#     delta = time - time_a
#     f = delta / diff

#     return gpxpy.mod_gpx.GPXTrackPoint(

#         time=time
#     )

#     print(f"a:{time_a}    b:{time_b}     t:{time}     f:{f}")
#     return point_a
#                     # TODO: If between two points -- approx position!
#                     # return mod_geo.Location(point.latitude, point.longitude)

def get_est_position_at_time(gpx: gpxpy.mod_gpx.GPX, time:datetime):
    points = []
    for track in gpx.tracks: 
        for segments in track.segments:

            first_time = segments.points[0].time
            last_time = segments.points[-1].time

            if first_time and time and last_time and not first_time <= time <= last_time:
                break

            for point in segments.points:
                if point.time and point.time >= time:
                    points.append(point)
                    break
    return points

def is_any_point_closer(points:list[gpxpy.mod_gpx.GPXTrackPoint], target:gpxpy.mod_gpx.GPXTrackPoint, distance:float) -> bool:
    for point in points:
        if point.distance_2d(target) < distance:
            return True
    return False


# def gen_desired_frame_indexes(insv_path: Path, start_time: datetime, gpx:gpxpy.mod_gpx.GPX, min_distance:float):
#     selected_points = []
#     selected_frames = []

#     discarded_frames_count = 0

#     for frame, pts in get_frames_and_pts(insv_path):
#         frame_time = start_time + pts

#         found_points = get_est_position_at_time(gpx, frame_time)
#         if len(found_points) > 0:
#             pos = found_points[0]

#             if is_any_point_closer(selected_points,pos,min_distance):
#                 discarded_frames_count +=1
#             else:
#                 selected_points.append(pos)
#                 selected_frames.append({
#                     "frame": frame,
#                     "frame_time": frame_time,
#                     "position": pos,
#                 })
    
#     return selected_frames,discarded_frames_count



def v360_yaw_filter_graph(width: int, height: int, pix_fmt: str, time_base, rotation_deg: float):
    """
    Build: buffer -> v360 -> buffersink
    v360 args: input=equirect:output=equirect:yaw=<rotation>
    """
    graph = av.filter.Graph()

    # Source (frames coming from decoder)
    src = graph.add(
        "buffer",
        args=(
            f"video_size={width}x{height}:"
            f"pix_fmt={pix_fmt}:"
            f"time_base={time_base.numerator}/{time_base.denominator}:"
            f"pixel_aspect=1/1"
        ),
    )

    # 360 Filter
    v360 = graph.add("v360",args=f"input=equirect:output=equirect:yaw={rotation_deg}",)

    # Sink (frames coming out of filter chain)
    sink = graph.add("buffersink")

    # Wire it up and configure
    src.link_to(v360)
    v360.link_to(sink)
    graph.configure()

    return graph, src, sink


def extract_frames(path:Path,out_path:Path, start_time:datetime, gpx:gpxpy.mod_gpx.GPX, min_distance:float, yaw:float):
    extracted_points = []
    extracted_frames = []
    
    av.logging.set_level(av.logging.ERROR)

    # Input stream
    container = av.open(path)
    input_stream = container.streams.video[0]

    # Enable multithreaded decoding
    cc = input_stream.codec_context
    cc.thread_type = "AUTO"   # or "FRAME" / "SLICE" depending on codec
    cc.thread_count = 0       # 0 = FFmpeg chooses (often == CPU cores)

    # # Output stream
    # out = av.open(out_path / "frames_%06d.jpg", mode="w", format="image2")
    # output_stream = out.add_stream("png")           # FFmpeg's JPEG encoder
    # output_stream.width = input_stream.width
    # output_stream.height = input_stream.height
    # output_stream.pix_fmt = "rgb24"                # common JPEG pix fmt
    # output_stream.options = {"compression_level": "8"}              # lower = better quality (2–5 typical)

    # # Enable multithreaded encoding
    # cc = output_stream.codec_context
    # cc.thread_type = "AUTO"   # or "FRAME" / "SLICE" depending on codec
    # cc.thread_count = 0       # 0 = FFmpeg chooses (often == CPU cores)

    filter_graph = None
    src = None
    sink = None

    process_start_time = datetime.now()

    print("")

    for index, frame in enumerate(container.decode(input_stream)):
        frame_time = start_time + timedelta(seconds=frame.time)
        found_points = get_est_position_at_time(gpx, frame_time)
        
        if len(found_points) > 0:
            if len(found_points) > 1:
                print (f"Warning: multiple gps positions were found for frame time {frame_time}, only first will be used")
            position = found_points[0]
        
            if not is_any_point_closer(extracted_points,position,min_distance):
                #extract frame

                if filter_graph == None:
                    filter_graph, src, sink = v360_yaw_filter_graph(
                            width=frame.width,
                            height=frame.height,
                            pix_fmt=frame.format.name,   
                            time_base=frame.time_base,
                            rotation_deg=yaw,
                        )

                src.push(frame)            
                filtered_frame = sink.pull()
                #for pkt in output_stream.encode(filtered_frame):
                #    out.mux(pkt)

                file_name = out_path / f"frames_{len(extracted_frames):0>5}.jpg"
                filtered_frame.to_image().save(file_name, quality=90, optimize=False)

                extracted_points.append(position)
                extracted_frames.append({
                    "frame_index": index,
                    "file_path": file_name,
                    "frame_time": frame_time,
                    "position": position,
                })
                    
        elapsed_time = datetime.now() - process_start_time
        print("extracting frames:: frame number: {}, extracted: {}, discarded: {}, effective fps: {:2f}, speed ratio: {:2f}".format(
            index,
            len(extracted_frames),
            index - len(extracted_frames),
            (len(extracted_frames) / frame.time) if frame.time > 0 else 0,
            frame.time / float(elapsed_time.seconds)
        ),end="\r")

    # flush output stream
    # for pkt in output_stream.encode():
    #     out.mux(pkt)
    # out.close()

    print("")
    print(f"frame extraction complete frames: {len(extracted_frames)} discarded: {index - len(extracted_frames)}")
    return extracted_frames

def offset_to_str(dt):
    off = dt.utcoffset()
    if off is None:
        return None

    total_seconds = int(off.total_seconds())
    sign = "+" if total_seconds >= 0 else "-"
    total_seconds = abs(total_seconds)

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60

    return f"{sign}{hours:02d}:{minutes:02d}"

def write_metadata(extracted_frames:list, out_path: Path):
    print("writing metadata csv")
    csv_path = out_path/"metadata.csv"
    csv_file = open(csv_path,"w")
    csv_file.write(
        "SourceFile,"
        "AllDates,"
        "SubSecTimeOriginal,"
        "SubSecTimeDigitized,"
        "OffsetTime,"
        "OffsetTimeOriginal,"
        "OffsetTimeDigitized,"
        "GPSLatitude,"
        "GPSLongitude,"
        "-XMP-GPano:ProjectionType,"
        "Make,"
        "Model,"
        "FocalLength"
        "\n")

    for extracted_frame in extracted_frames:
        time_stamp_whole = extracted_frame["frame_time"].strftime("%Y:%m:%d %H:%M:%S")
        time_stamp_ms = int(round(extracted_frame["frame_time"].microsecond / 1000.0))
        time_zone_offset = offset_to_str(extracted_frame["frame_time"])
        csv_file.write(
            f"{extracted_frame["file_path"]},"
            f"{time_stamp_whole},"
            f"{time_stamp_ms:03d},"
            f"{time_stamp_ms:03d},"
            f"{time_zone_offset},"
            f"{time_zone_offset},"
            f"{time_zone_offset},"
            f"{extracted_frame["position"].latitude},"
            f"{extracted_frame["position"].longitude},"
            "equirectangular,"
            "Insta360,"
            "X4,"
            "1.2"
            "\n"
        )

    csv_file.close()
    print("csv written")

    print("applying metadata")    
    cmd = [
        "exiftool",
        "-api", "largefilesupport=1",
        "-overwrite_original",
        f"-csv={csv_path}",
        out_path
    ]
    result = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr)



@click.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--yaw",default=0, help="the Yaw adjustment to be applied to the video")
@click.option("--min_distance",default=5, help="minimum distance between frames in meters")
@click.option("--out_path",default="", help="output path")
def main(path,yaw,min_distance,out_path):

    path        = Path(path)
    mp4_path    = path.with_suffix(".mp4")
    out_path    = path.with_suffix("") if out_path == "" else Path(out_path)
    gpx_path    = out_path / f"{out_path.name}.gpx"

    #create output directory
    if out_path.exists() == True:
        print(f"Warning: output directory already exists ({out_path})")
    out_path.mkdir(parents=True,exist_ok=True)

    #extract gpx
    if extract_gps(path,gpx_path) != 0:
        print("Error: Extracting GPS failed")
        return 2

    #load gpx file
    gpx_file = open(gpx_path, 'r')
    gpx = gpxpy.parse(gpx_file)

    #get start time
    start_time = get_start_time(path)
    if start_time == None:
        print("Error: Failed to extract start time from input file")
        return 3
    print(f"Start time: {start_time}")

    extracted_frames = extract_frames(mp4_path,out_path,start_time,gpx,min_distance,yaw)

    write_metadata(extracted_frames, out_path)
    # #get duration
    # duration = get_duration(path)
    # if duration == None:
    #     print("Error: Failed to extract duration from input file")
    #     return 4
    # print(f"Duration: {duration}")

    # #end time
    # end_time = start_time + duration
    # print(f"End Time: {end_time}")

    # desired_frames,discarded_count = gen_desired_frame_indexes(path,start_time,gpx,min_distance)

    # print(f"Desired: {len(desired_frames)}, discarded: {discarded_count}")

   
    return 0