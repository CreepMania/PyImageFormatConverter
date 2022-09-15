import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from glob import glob
from pathlib import Path
from typing import Any, Dict

from PIL import Image
from tqdm import tqdm


@dataclass
class Config:
    def __init__(self, args: Dict[str, Any]):
        self.NB_WORKERS = args['nb_workers']
        self.DESTINATION_FOLDER = args['dest']
        self.SOURCE_FOLDER = args['source']
        self.DESTINATION_FILE_TYPE = args['dest_file_type']
        self.SOURCE_FILE_TYPE = args['file_type']
        self.IMAGE_QUALITY = args['quality']
        self.OPTIMIZE = args['optimize']
        self.PROGRESSIVE = args['progressive']
        self.IS_RECURSIVE = args['recursive']

    NB_WORKERS: int
    DESTINATION_FOLDER: Path
    DESTINATION_FILE_TYPE: str
    SOURCE_FOLDER: Path
    SOURCE_FILE_TYPE: str
    IS_RECURSIVE: bool
    IMAGE_QUALITY: int
    OPTIMIZE: bool
    PROGRESSIVE: bool


def check_config(config: Config):
    if not config.SOURCE_FILE_TYPE.startswith('.'):
        config.SOURCE_FILE_TYPE = f".{config.SOURCE_FILE_TYPE}"
    if not config.DESTINATION_FILE_TYPE.startswith('.'):
        config.DESTINATION_FILE_TYPE = f".{config.DESTINATION_FILE_TYPE}"
    if not config.SOURCE_FOLDER.is_dir():
        raise SystemExit("Error: Source path does not lead to a directory")
    if not config.DESTINATION_FOLDER.is_dir():
        raise SystemExit("Error: Destination path does not lead to a directory")
    if not config.SOURCE_FOLDER.exists():
        raise SystemExit("Error: Source folder does not exist")
    if not config.DESTINATION_FOLDER.exists():
        raise SystemExit("Error: Destination folder does not exist")
    if not glob(f"{config.SOURCE_FOLDER}/**/*{config.SOURCE_FILE_TYPE}", recursive=config.IS_RECURSIVE):
        raise SystemExit(f"Error: Source folder does not contain any file of type {config.SOURCE_FILE_TYPE}")
    if config.DESTINATION_FILE_TYPE not in Image.registered_extensions().keys():
        raise SystemExit(f"Error: Desired conversion format is not supported")
    if config.SOURCE_FILE_TYPE not in Image.registered_extensions().keys():
        raise SystemExit(f"Error: Source image format is not supported")


def configure_argument_parser() -> Config:
    parser = argparse.ArgumentParser(description='Convert images from one format to another with ease')
    parser.add_argument('source',  type=Path, help='source folder')
    parser.add_argument('dest',  type=Path, help='destination folder')
    parser.add_argument('file_type', type=str, help='type of files to convert')
    parser.add_argument('dest_file_type', type=str, help='desired file type')
    parser.add_argument('--nb-workers', type=int, default=min(32, (os.cpu_count() or 1) + 4),
                        help='max number of workers to use (default: %(default)s)')
    parser.add_argument('--optimize', action='store_true',
                        help='optimize images (default: %(default)s)', default=True)
    parser.add_argument('--progressive', action='store_true',
                        help='convert to progressive image (default: %(default)s)', default=True)
    parser.add_argument('--quality', action='store_const',
                        help='image quality (default: %(const)s)', const=80, default=80)
    parser.add_argument('--recursive', action='store_true',
                        help='recursively search for files (default: %(default)s)', default=True)
    args = vars(parser.parse_args())
    return Config(args)


def __handle_jpg_transparency(image: Image) -> Image:
    return image.convert('RGB')


def convert(source_file_path: Path, config: Config):
    dest_path = Path(f"{config.DESTINATION_FOLDER.absolute()}/{source_file_path.stem}{config.DESTINATION_FILE_TYPE}")
    with Image.open(source_file_path) as image:
        try:
            extension_name = Image.registered_extensions().get(dest_path.suffix)
            if dest_path.exists():
                tqdm.write(f"Warning: file {dest_path} already exists", nolock=True)

            image.save(dest_path, extension_name,
                       quality=config.IMAGE_QUALITY,
                       optimize=config.OPTIMIZE,
                       progressive=config.PROGRESSIVE)
        except OSError as ex:
            if 'RGBA' in str(ex):
                tqdm.write(f"Warning: file {source_file_path} has a transparency layer, message: {ex}")
                image = __handle_jpg_transparency(image)
            image.save(dest_path, extension_name,
                       quality=config.IMAGE_QUALITY,
                       optimize=config.OPTIMIZE,
                       progressive=config.PROGRESSIVE)
        except Exception as ex:
            tqdm.write(f"Error: file: {source_file_path}, message: {ex}", nolock=True)


if __name__ == "__main__":
    config = configure_argument_parser()
    check_config(config)
    all_files = [Path(file) for file in glob(f"{config.SOURCE_FOLDER.absolute()}/**/*{config.SOURCE_FILE_TYPE}",
                                             recursive=config.IS_RECURSIVE)]
    with ThreadPoolExecutor(max_workers=config.NB_WORKERS) as executor:
        futures = [executor.submit(convert, file, config) for file in all_files]
        list(tqdm(as_completed(futures), total=len(futures)))
    tqdm.write('Done.')
