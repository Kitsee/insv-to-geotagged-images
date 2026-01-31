#!/usr/bin/env python3

import sys, os, datetime, subprocess, glob, json
from pathlib import Path

def extract_gps(insv_path, output_path):
    frame_name = Path(output_path).name
    gps_path = Path(output_path) / f"{frame_name}.gpx"

    print("Extracting GPX")
    outfile = open(gps_path, "w", encoding="utf-8")
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
 
 
def extract_frames(mp4_path, output_path, rotation, fps):
    print(f"Extracting Frames at {fps} fps")

    output_file_path = output_path / "output.txt"
    frame_times_path = output_path / "frame_times.txt"
    frame_name = output_path.name

    #extract frames
    outputFile = open(output_file_path, "w", encoding="utf-8")
    cmd = [
        "ffmpeg",
        "-i", mp4_path,
        "-vf",
        f"v360=input=equirect:output=equirect:yaw={rotation},fps={fps},showinfo",
        "-qscale:v", "2",
        f"{output_path}/{frame_name}_%06d.jpg"
    ]
    result = subprocess.run(cmd, stdout=sys.stdout, stderr=outputFile)
    outputFile.close()

    if result.returncode != 0:
        return result.returncode

    #process frame times file

    #grep to get lines with pts time
    grep_proc = subprocess.Popen(
        [
            "grep", 
            "pts_time", 
            output_file_path
        ],
        stdout=subprocess.PIPE,
        text=True
    )

    #filter for just the value
    frame_times_file = open(frame_times_path, "w", encoding="utf-8")
    subprocess.run(
        ["sed", "-E", r"s/.*pts_time:([0-9.]+).*/\1/"],
        stdin=grep_proc.stdout,
        stdout=frame_times_file,
        text=True,
        check=True
    )
    frame_times_file.close()
    return 0




def get_start_time(path):
    #get start time of mp4
    cmd = [
        "exiftool", 
        "-api", "largefilesupport=1",
        "-json", 
        "-api", 
        "QuickTimeUTC", 
        "-CreateDate", 
        "-MediaCreateDate", 
        path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(result.stdout)[0]
    return data.get("CreateDate") or data.get("MediaCreateDate")



def get_frame_deltas(path):
    deltas = []
    frame_times_file = open(path, "r", encoding="utf-8")
    for line in frame_times_file:
        line = line.strip()
        if not line:
            continue
        deltas.append(float(line))
    frame_times_file.close()
    return deltas



def set_time_stamps(mp4_path, output_path):
    print("Setting Time Stamps")

    frame_times_path = output_path / "frame_times.txt"

    start_time_str = get_start_time(mp4_path)
    print("mp4 start time: " + start_time_str)
    start_time = datetime.datetime.fromisoformat(start_time_str.replace(":", "-", 2))
    time_zone_offset = start_time_str[-6:]

    frame_deltas = get_frame_deltas(frame_times_path)

    frames = sorted(glob.glob(os.path.join(output_path, "*.jpg")))

    if len(frame_deltas) != len(frames):
        print("error length of frame times files doesnt match number of frames")
        return -1
    
    for i, frame_path in enumerate(frames):
        delta_time = start_time + datetime.timedelta(seconds=frame_deltas[i])
        time_stamp_whole = delta_time.strftime("%Y:%m:%d %H:%M:%S")
        time_stamp_ms = int(round(delta_time.microsecond / 1000.0))
        #print(f"  {i+1}/{len(frames)}  →  {time_stamp_whole}:{time_stamp_ms}")

        cmd = [
            "exiftool", 
            "-overwrite_original",
            "-XMP-GPano:ProjectionType=equirectangular",
            "-Make=Insta360",
            "-Model=X4",
            "-FocalLength=1.2",
            f"-AllDates={time_stamp_whole}",
            f"-SubSecTimeOriginal={time_stamp_ms:03d}",
            f"-SubSecTimeDigitized={time_stamp_ms:03d}",
            f"-OffsetTime={time_zone_offset}",
            f"-OffsetTimeOriginal={time_zone_offset}",
            f"-OffsetTimeDigitized={time_zone_offset}",
            frame_path
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    return 0


def geotag_frames(output_path):
    print("Geotagging Frames")
    frame_name = Path(output_path).name
    gps_path = Path(output_path) / f"{frame_name}.gpx"

    cmd = [
        "exiftool",
        "-overwrite_original",
        f"-geotag={gps_path}",
        "-Geotime<${SubSecDateTimeOriginal}-00:00",
        f"{output_path}/"
    ]
    return subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr).returncode

def cleanup(output_path):
    Path(output_path / "output.txt").unlink()
    Path(output_path / "frame_times.txt").unlink()


def process_file(insv_path,yaw_rotation,fps):
    
    insv_path = Path(insv_path)

    mp4_only_mode=False

    #check insv exists
    if insv_path.suffix != '.insv':
        if insv_path.suffix == 'mp4':
            mp4_only_mode = True
        else:
            print("Error: input is not a .insv file")
            return 1
    
    if insv_path.exists() == False and mp4_only_mode == False:
        print(f"Error: insv file  does not exist ({insv_path})")
        return 1
    
    #check mp4 exists
    mp4_path = insv_path.with_suffix(".mp4")
    if mp4_path.exists() == False:
        print(f"Error: mp4 with matching name missing ({mp4_path})")
        return 1
    
   
    if mp4_only_mode == False:
        #create output directory
        output_path = insv_path.with_suffix("")
        if output_path.exists() == True:
            print(f"Error: output directory already exists ({output_path})")
            return 1
        output_path.mkdir(parents=True)

        if extract_gps(insv_path,output_path) != 0:
            print("Error: Extracting GPS failed")
            return 2
        
    if extract_frames(mp4_path,output_path,yaw_rotation,fps) != 0:
        print("Error: Extracting Frames failed")
        return 3
    
    if set_time_stamps(mp4_path,output_path) != 0:
        print("Error: Failed setting time stamps")
        return 4
    
    if geotag_frames(output_path) != 0:
        print("Error: geotagging frames")
        return 5
    
    cleanup(output_path)


def main():
    
    path_str = sys.argv[1]
    path = Path(path_str)
    yaw_rotation = sys.argv[2]

    fps = 1
    if len(sys.argv) >= 4:
        fps = sys.argv[3]

    print(f"path: {path}")
    print(f"yaw:  {yaw_rotation}")
    print(f"fps:  {fps}")

    if path.is_file():
        #single file mode
        process_file(path,yaw_rotation,2)

    else:
        #multi file mode
        insta_files = sorted(glob.glob(os.path.join(path, "*.insv")))
        print(f"files found:")
        for i, file_path in enumerate(insta_files):
            print(f" - {i} -> {file_path}")
            
        for i, file_path in enumerate(insta_files):
            print("--------------------------------------------------------")
            print(f"processing input file {i} -> {file_path}")
            process_file(file_path,yaw_rotation,fps)

    return 0