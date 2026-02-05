#!/usr/bin/env python3

import click
import sys,subprocess
import gpxpy
from pathlib import Path
from datetime import datetime, timedelta,timezone
import av


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


def get_gps_start_time(insv_path : Path) -> datetime:
    cmd = [
        "exiftool",
        "-api", "largefilesupport=1",
        "-ee",
        "-p", "'$GPSDateTime'",
        insv_path
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    output = result.stdout.strip()

    lines = [l for l in output.splitlines() if l.strip()]
    if lines:
        return datetime.fromisoformat(lines[0][1:-1].replace("Z", "+00:00").replace(":", "-", 2))

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
    
    assumed_utc = False
    start_time = datetime.strptime(ts.partition('\n')[0], "%Y:%m:%d %H:%M:%S") # EXIF format: YYYY:MM:DD HH:MM:SS
    if start_time.tzinfo is None:
        assumed_utc = True
        start_time = start_time.replace(tzinfo=timezone.utc)

    gps_start_time = get_gps_start_time(insv_path)
    if (gps_start_time - start_time).total_seconds() < 1.0:
        start_time = gps_start_time
        print ("GPS start time is within 1 second of video start time. using GPS start time as it normally has better accuracy")
    elif assumed_utc:
        print("Warning: start time has no time zone, assuming UTC.")

    return start_time


def get_est_position_at_time(gpx: gpxpy.mod_gpx.GPX, time:datetime) -> list[gpxpy.mod_gpx.GPXTrackPoint]:
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


def should_extract_frame(points:list[gpxpy.mod_gpx.GPXTrackPoint], target:gpxpy.mod_gpx.GPXTrackPoint, speed_arg, distance) -> bool:

    #convert to mph
    speed = speed_arg * 2.237

    min_distance = None
    if isinstance(distance, float):
        #min mode
        min_distance = distance
    else:
        #adaptive mode
        min_dist,min_speed,max_dist,max_speed = distance

        t = (speed - min_speed) / (max_speed - min_speed)
        t = min(t, 1)
        t = max(t, 0)
        min_distance = min_dist + t * (max_dist - min_dist)
    
    #go backwards as the points at the end of the list are far more likely to be close to the current point
    for point in reversed(points):
        if point.distance_2d(target) < min_distance:
            return False
    return True



def extract_frames(path:Path,out_path:Path, start_time:datetime, gpx:gpxpy.mod_gpx.GPX, min_distance, yaw:float):
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

    filter_graph = None
    src = None
    sink = None

    process_start_time = datetime.now()

    for index, frame in enumerate(container.decode(input_stream)):
        frame_time = start_time + timedelta(seconds=frame.time)
        found_points = get_est_position_at_time(gpx, frame_time)
        speed = 0.0

        if len(found_points) > 0:
            if len(found_points) > 1:
                print (f"Warning: multiple gps positions were found for frame time {frame_time}, only first will be used")

            position = found_points[0]

            #caculate speed
            speed = 0.0
            time_offset = timedelta(seconds=-1)
            speed_points = get_est_position_at_time(gpx, frame_time + time_offset)
            if len(speed_points) > 0:
                speed = position.speed_between(speed_points[0])
                if speed == None: speed = 0.0


            if should_extract_frame(extracted_points,position,speed,min_distance):
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

                file_name = out_path / f"{out_path.name}_{len(extracted_frames):0>6}.jpg"
                filtered_frame.to_image().save(file_name, quality=90, optimize=True)

                extracted_points.append(position)
                extracted_frames.append({
                    "frame_index": index,
                    "file_path": file_name,
                    "frame_time": frame_time,
                    "position": position,
                })

            last_frame_position = position
                    
        elapsed_time = datetime.now() - process_start_time
        print("frame: {}, extracted: {}, discarded: {}, eff fps: {:.2f}, process speed: {:.2f}, gnd speed {:.2f} mph        ".format(
            index+1,
            len(extracted_frames),
            index - len(extracted_frames),
            ((len(extracted_frames) / frame.time) +1) if frame.time > 0 else 0,
            frame.time / float(elapsed_time.seconds) if elapsed_time.seconds > 0 else 0.0,
            speed * 2.237
        ),end="\r")

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
        #time
        "AllDates,"
        "SubSecTimeOriginal,"
        "SubSecTimeDigitized,"
        "OffsetTime,"
        "OffsetTimeOriginal,"
        "OffsetTimeDigitized,"
        #gps
        "GPSLatitude,"
        "GPSLongitude,"
        "GPSLatitudeRef,"
        "GPSLongitudeRef,"
        "GPSImgDirection,"
        #constants
        "XMP-GPano:ProjectionType,"
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
            #time
            f"{time_stamp_whole},"
            f"{time_stamp_ms:03d},"
            f"{time_stamp_ms:03d},"
            f"{time_zone_offset},"
            f"{time_zone_offset},"
            f"{time_zone_offset},"
            #gps
            f"{extracted_frame["position"].latitude},"
            f"{abs(extracted_frame["position"].longitude)},"
            "N,"
            f"{"E" if extracted_frame["position"].longitude >= 0 else "W"},"
            f"{extracted_frame["position"].course},"
            #constants
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

    #cleanup csv
    csv_path.unlink()

    if result.returncode != 0:
        return False
    
    return True


def load_gpx(path : Path) -> gpxpy.mod_gpx.GPX:
    gpx_file = open(path, 'r')
    gpx = gpxpy.parse(gpx_file)

    #load extension fields
    for track in gpx.tracks:
        for segment in track.segments:
            for point in segment.points:

                for extension in point.extensions:
                    if extension.tag == 'heading':
                        point.course = float(extension.text)
    return gpx


@click.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--out_path",default="", help="output path")
@click.option("--yaw",default=0, help="the Yaw adjustment to be applied to the video")
@click.option("--min_distance",default=5, help="minimum distance between frames in meters")
@click.option("--adaptive_distance",nargs=4, type=float, help="varys min_distance based on speed, takes 4 vales min_dis @ min_speed, min_ids @ max_speed")
def main(path,out_path,yaw,min_distance,adaptive_distance):
    min_distance = float(min_distance) #force it to a float

    if adaptive_distance != None:
        a,b,c,d = adaptive_distance
        print(f"adaptive distance, {a}m @ {b}mph scaling up to {c}m @ {d}mph ")
        min_distance = adaptive_distance


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
    gpx = load_gpx(gpx_path)

    #get start time
    start_time = get_start_time(path)
    if start_time == None:
        print("Error: Failed to extract start time from input file")
        return 3
    
    start_time += timedelta(seconds=-1) # why is it 1sec off

    print(f"Start time: {start_time}")

    extracted_frames = extract_frames(mp4_path,out_path,start_time,gpx,min_distance,yaw)

    if not write_metadata(extracted_frames, out_path):
        print("Error: Failed to write metadata to frames")
        return 4

    return 0