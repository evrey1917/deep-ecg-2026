from pathlib import Path
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def validate_path_exists(
    path: Path, is_dir: bool = True, path_name: str = "Path"
) -> bool:
    """
    Проверяет существование пути и логирует ошибки

    :param path: Путь для проверки
    :param is_dir: True если проверяется директория, False если файл
    :param path_name: Название пути для логирования
    :return: True если путь существует и соответствует типу, иначе False
    """
    if not path.exists():
        logging.error(f"{path_name} не существует: {path}")
        return False

    if is_dir and not path.is_dir():
        logging.error(f"{path_name} не является директорией: {path}")
        return False

    if not is_dir and not path.is_file():
        logging.error(f"{path_name} не является файлом: {path}")
        return False

    return True


def find_csv_dir_files_china(project_dir: Path):
    """
    Function for get Paths from china_12_lead dataset

    :param Path project_dir: Main project dir

    :return records_dir (Path): Dir with records
    :return metadata_file (Path): File with metadata
    :return code_file (Path): File with AHA codes
    """

    # Проверка основного проекта
    if not validate_path_exists(
        project_dir, is_dir=True, path_name="Project directory"
    ):
        raise FileNotFoundError(f"Project directory not found: {project_dir}")

    data_dir = project_dir / "data"
    if not validate_path_exists(data_dir, is_dir=True, path_name="Data directory"):
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    dataset_dir = data_dir / "china_12_lead"
    if not validate_path_exists(
        dataset_dir, is_dir=True, path_name="Dataset directory"
    ):
        raise FileNotFoundError(f"Dataset directory not found: {dataset_dir}")

    records_dir = dataset_dir / "records"
    if not validate_path_exists(
        records_dir, is_dir=True, path_name="Records directory"
    ):
        raise FileNotFoundError(f"Records directory not found: {records_dir}")

    metadata_file = dataset_dir / "metadata.csv"
    if not validate_path_exists(metadata_file, is_dir=False, path_name="Metadata file"):
        raise FileNotFoundError(f"Metadata file not found: {metadata_file}")

    code_file = dataset_dir / "code.csv"
    if not validate_path_exists(code_file, is_dir=False, path_name="Code file"):
        raise FileNotFoundError(f"Code file not found: {code_file}")

    logging.info("Successfully validated all paths for china_12_lead dataset")
    return records_dir, metadata_file, code_file


def find_csv_dir_files_ptb_xl(project_dir: Path):
    """
    Function for get Paths from ptb_xl dataset

    :param Path project_dir: Main project dir

    :return records100_dir (Path): Dir with LOW resolution records (100 Hz)
    :return records500_dir (Path): Dir with HIGH resolution records (500 Hz)
    :return metadata_file (Path): File with metadata
    :return code_file (Path): File with inner codes (and AHA codes)
    """

    # Проверка основного проекта
    if not validate_path_exists(
        project_dir, is_dir=True, path_name="Project directory"
    ):
        raise FileNotFoundError(f"Project directory not found: {project_dir}")

    data_dir = project_dir / "data"
    if not validate_path_exists(data_dir, is_dir=True, path_name="Data directory"):
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    dataset_dir = data_dir / "ptb_xl"
    if not validate_path_exists(
        dataset_dir, is_dir=True, path_name="Dataset directory"
    ):
        raise FileNotFoundError(f"Dataset directory not found: {dataset_dir}")

    records100_dir = dataset_dir / "records100"
    records500_dir = dataset_dir / "records500"

    # Проверяем хотя бы одну из директорий с записями
    records_found = False
    if validate_path_exists(
        records100_dir, is_dir=True, path_name="Records100 directory"
    ):
        records_found = True
        logging.info(f"Found low resolution records: {records100_dir}")

    if validate_path_exists(
        records500_dir, is_dir=True, path_name="Records500 directory"
    ):
        records_found = True
        logging.info(f"Found high resolution records: {records500_dir}")

    if not records_found:
        raise FileNotFoundError(
            f"No record directories found in {dataset_dir}. "
            f"Checked: {records100_dir}, {records500_dir}"
        )

    metadata_file = dataset_dir / "ptbxl_database.csv"
    if not validate_path_exists(metadata_file, is_dir=False, path_name="Metadata file"):
        raise FileNotFoundError(f"Metadata file not found: {metadata_file}")

    code_file = dataset_dir / "scp_statements.csv"
    if not validate_path_exists(code_file, is_dir=False, path_name="Code file"):
        raise FileNotFoundError(f"Code file not found: {code_file}")

    logging.info("Successfully validated all paths for ptb_xl dataset")
    return records100_dir, records500_dir, metadata_file, code_file


def find_dirs_mit_bih(project_dir: Path):
    """
    Function for get Paths from mit_bih dataset

    :param Path project_dir: Main project dir

    :return records_dir (Path): Dir with records
    :return metadata_file (Path): File with metadata
    :return code_file (Path): File with AHA codes
    """

    # Проверка основного проекта
    if not validate_path_exists(
        project_dir, is_dir=True, path_name="Project directory"
    ):
        raise FileNotFoundError(f"Project directory not found: {project_dir}")

    data_dir = project_dir / "data"
    if not validate_path_exists(data_dir, is_dir=True, path_name="Data directory"):
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    mit_bih_dir = data_dir / "mit_bih"
    if not validate_path_exists(
        mit_bih_dir, is_dir=True, path_name="MIT-BIH directory"
    ):
        raise FileNotFoundError(f"MIT-BIH directory not found: {mit_bih_dir}")

    x_mit_bih_dir = mit_bih_dir / "x_mitdb"
    if not validate_path_exists(
        x_mit_bih_dir, is_dir=True, path_name="X-MITDB directory"
    ):
        # Возможно, файлы находятся в корневой директории MIT-BIH
        logging.warning(f"X-MITDB directory not found: {x_mit_bih_dir}")
        logging.info("Checking MIT-BIH root directory for files...")

        # Проверяем наличие хотя бы одного файла в корневой директории
        files = list(mit_bih_dir.glob("*"))
        if not files:
            raise FileNotFoundError(
                f"No files found in MIT-BIH directory: {mit_bih_dir}"
            )

        logging.info(f"Found {len(files)} files/directories in MIT-BIH root directory")

    logging.info("Successfully validated MIT-BIH dataset paths")
    return mit_bih_dir, x_mit_bih_dir


# Дополнительная функция для удобного использования
def safe_find_paths(dataset_name: str, project_dir: Path):
    """
    Безопасное получение путей к данным с обработкой ошибок

    :param dataset_name: Имя датасета ('china', 'ptb_xl', 'mit_bih')
    :param project_dir: Основная директория проекта
    :return: Кортеж с путями к данным
    """
    try:
        if dataset_name.lower() == "china":
            return find_csv_dir_files_china(project_dir)
        elif dataset_name.lower() == "ptb_xl":
            return find_csv_dir_files_ptb_xl(project_dir)
        elif dataset_name.lower() == "mit_bih":
            return find_dirs_mit_bih(project_dir)
        else:
            raise ValueError(
                f"Unknown dataset name: {dataset_name}. "
                f"Available: 'china', 'ptb_xl', 'mit_bih'"
            )
    except (FileNotFoundError, ValueError) as e:
        logging.error(f"Failed to find paths for dataset '{dataset_name}': {e}")
        raise
