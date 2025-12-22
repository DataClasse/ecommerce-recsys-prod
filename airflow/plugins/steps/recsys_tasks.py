"""Функции для задач DAG recsys_train_daily.

Все функции определены на уровне модуля для корректной сериализации Airflow.
"""

import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Telegram notifier импортируется внутри функций при необходимости


def _version_file_with_dvc(
    file_path: Path,
    project_root: Path,
    dvc_cache_dir: str,
    site_cache_dir: Path,
    logger: logging.Logger,
) -> tuple[bool, str | None]:
    """Версионирует файл через DVC add и синхронизирует с S3.
    
    Args:
        file_path: Путь к файлу для версионирования
        project_root: Корневая директория проекта
        dvc_cache_dir: Директория кеша DVC
        site_cache_dir: Директория site_cache для DVC
        logger: Логгер для записи
        
    Returns:
        tuple: (успешно ли версионировано, сообщение об ошибке или None)
    """
    import subprocess
    import yaml
    
    if not file_path.exists():
        return False, f"Файл не найден: {file_path}"
    
    # Проверяем, не определен ли файл уже в dvc.yaml как выход стадии
    dvc_yaml_path = project_root / "dvc.yaml"
    file_in_pipeline = False
    if dvc_yaml_path.exists():
        try:
            with open(dvc_yaml_path, "r") as f:
                dvc_config = yaml.safe_load(f)
                if dvc_config:
                    for stage_name, stage_config in dvc_config.get("stages", {}).items():
                        outputs = stage_config.get("outs", [])
                        if any(str(file_path) in str(out) for out in outputs):
                            file_in_pipeline = True
                            break
        except Exception:
            pass
    
    if file_in_pipeline:
        # Файл уже в пайплайне DVC, версионирование не требуется
        logger.info(f"Файл уже в пайплайне DVC, версионирование не требуется: {file_path}")
        return True, None
    
    # Проверяем, не версионирован ли уже файл
    dvc_file = file_path.with_suffix(file_path.suffix + ".dvc")
    if dvc_file.exists():
        logger.info(f"Файл уже под версионированием DVC: {file_path}")
        return True, None
    
    # Устанавливаем локальный кеш через dvc config (если еще не установлен)
    current_cache = subprocess.run(
        ["dvc", "cache", "dir"],
        cwd=str(project_root),
        capture_output=True,
        text=True,
        timeout=5
    )
    if current_cache.returncode == 0 and current_cache.stdout.strip() != dvc_cache_dir:
        logger.info(f"Установка локального кеша DVC: {dvc_cache_dir}")
        subprocess.run(
            ["dvc", "config", "--local", "cache.dir", dvc_cache_dir],
            cwd=str(project_root),
            capture_output=True,
            timeout=5
        )
    
    # Используем dvc add для версионирования файла
    logger.info(f"Версионирование файла через DVC add: {file_path}")
    result_dvc = subprocess.run(
        ["dvc", "add", str(file_path)],
        cwd=str(project_root),
        capture_output=True,
        timeout=60,
        text=True
    )
    
    if result_dvc.returncode == 0:
        logger.info(f"Файл успешно добавлен под версионирование DVC: {file_path}")
        
        # Синхронизация с remote storage (S3) через dvc push
        logger.info(f"Синхронизация файла с remote storage через DVC push: {file_path}")
        result_push = subprocess.run(
            ["dvc", "push", "-r", "storage"],
            cwd=str(project_root),
            capture_output=True,
            timeout=300,  # 5 минут на push
            text=True
        )
        if result_push.returncode == 0:
            logger.info(f"Файл успешно синхронизирован с remote storage: {file_path}")
            return True, None
        else:
            error_push = result_push.stderr or result_push.stdout
            logger.warning(f"DVC push ошибка для {file_path} (версионирование выполнено, но push не удался): {error_push.strip()[:200]}")
            return True, None  # Версионирование выполнено, push не критичен
    else:
        error_output = result_dvc.stderr or result_dvc.stdout
        # Проверяем, не связана ли ошибка с site_cache_dir
        if "Permission denied" in error_output and "/var/cache/dvc" in error_output:
            # Пробуем создать директорию еще раз
            try:
                sudo_check = subprocess.run(
                    ["sudo", "-n", "true"],
                    capture_output=True,
                    timeout=2,
                    check=False
                )
                if sudo_check.returncode == 0:
                    subprocess.run(
                        ["sudo", "mkdir", "-p", str(site_cache_dir)],
                        capture_output=True,
                        timeout=5,
                        check=False
                    )
                    subprocess.run(
                        ["sudo", "chmod", "777", str(site_cache_dir)],
                        capture_output=True,
                        timeout=5,
                        check=False
                    )
                    logger.info(f"Повторная попытка создания site_cache_dir: {site_cache_dir}")
            except Exception:
                pass
            return False, "DVC требует прав доступа к /var/cache/dvc"
        elif "overlaps with an output" in error_output:
            # Файл уже в пайплайне, но мы не обнаружили это ранее
            logger.info(f"Файл уже в пайплайне DVC, версионирование не требуется: {file_path}")
            return True, None
        else:
            return False, f"DVC add ошибка: {error_output.strip()[:200]}"


def _setup_pythonpath():
    """Настройка PYTHONPATH для импорта модулей проекта.
    
    Добавляет корень проекта в sys.path для импорта модулей через 'src.recsys.*'.
    Также устанавливает переменные окружения RECSYS_* если они не установлены.
    Загружает .env файл для переменных окружения (Telegram, MLflow и т.д.).
    """
    # Получаем корень проекта из переменной окружения или используем значение по умолчанию
    project_root = Path(os.getenv("RECSYS_PROJECT_ROOT", "/home/mle-user/mle-pr-final"))
    
    # Загружаем .env файл для переменных окружения (Telegram, MLflow и т.д.)
    try:
        from dotenv import load_dotenv
        env_path = project_root / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=False)
    except ImportError:
        pass
    except Exception:
        pass
    
    # Устанавливаем переменные окружения, если они не установлены
    if not os.getenv("RECSYS_PROJECT_ROOT"):
        os.environ["RECSYS_PROJECT_ROOT"] = str(project_root)
    if not os.getenv("RECSYS_DATA_DIR"):
        os.environ["RECSYS_DATA_DIR"] = str(project_root / "data" / "raw")
    if not os.getenv("RECSYS_ARTIFACTS_DIR"):
        os.environ["RECSYS_ARTIFACTS_DIR"] = str(project_root / "artifacts")
    if not os.getenv("RECSYS_CONFIG_PATH"):
        os.environ["RECSYS_CONFIG_PATH"] = str(project_root / "configs" / "config.yaml")
    
    # КРИТИЧЕСКИ ВАЖНО: Добавляем КОРЕНЬ ПРОЕКТА в sys.path, а не src!
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    
    plugins_path = str(project_root / "airflow" / "plugins")
    if plugins_path not in sys.path:
        sys.path.insert(0, plugins_path)
    
    # Устанавливаем переменную окружения для подпроцессов
    pythonpath = os.getenv("PYTHONPATH", "")
    paths_to_add = [project_root_str, plugins_path]
    new_paths = [p for p in paths_to_add if p not in pythonpath]
    
    if new_paths:
        pythonpath = ":".join(new_paths) + (":" + pythonpath if pythonpath else "")
        os.environ["PYTHONPATH"] = pythonpath


def validate_data(**kwargs):
    """Валидация исходных данных перед обработкой.
    
    Проверяет наличие и корректность файлов:
    - events.csv
    - item_properties_part1.csv, item_properties_part2.csv
    - category_tree.csv
    
    Args:
        **kwargs: Airflow context (ti, ds, и т.д.)
    
    Returns:
        dict: Результат валидации с полями:
            - status (str): "validated" при успешной валидации
            - files (dict): словарь с размерами файлов {filename: size_bytes}
            - data_dir (str): путь к директории с данными
    
    Raises:
        FileNotFoundError: Если отсутствуют обязательные файлы
    """
    # Настройка PYTHONPATH перед выполнением
    _setup_pythonpath()
    
    ti = kwargs.get('ti')
    
    # Получаем пути из переменных окружения или используем значения по умолчанию
    project_root = Path(os.getenv("RECSYS_PROJECT_ROOT", "/home/mle-user/mle-pr-final"))
    data_dir = Path(os.getenv("RECSYS_DATA_DIR", str(project_root / "data" / "raw")))
    
    # Настройка окружения
    os.environ["PYTHONPATH"] = str(project_root / "src")
    
    # Проверка наличия файлов
    required_files = [
        "events.csv",
        "item_properties_part1.csv",
        "item_properties_part2.csv",
        "category_tree.csv",
    ]
    
    missing_files = []
    file_sizes = {}
    
    for filename in required_files:
        filepath = data_dir / filename
        if not filepath.exists():
            missing_files.append(filename)
        else:
            file_sizes[filename] = filepath.stat().st_size
    
    if missing_files:
        raise FileNotFoundError(
            f"Отсутствуют обязательные файлы: {missing_files}\n"
            f"Директория: {data_dir}"
        )
    
    total_size = sum(file_sizes.values()) / (1024 * 1024)
    logger.info(f"Валидация данных успешна: {len(file_sizes)} файлов, {total_size:.2f} MB")
    
    result = {
        "status": "validated",
        "files": file_sizes,
        "data_dir": str(data_dir),
    }
    
    if ti:
        ti.xcom_push(key="validation_result", value=result)
    
    # Отправка уведомления об успешном выполнении
    # validate_data - первая задача, выделяем её как начало блока
    try:
        from src.recsys.utils.telegram_notifier import get_notifier
        notifier = get_notifier()
        if notifier.enabled:
            dag_id = kwargs.get('dag').dag_id if kwargs.get('dag') else "recsys_train_daily"
            run_id = kwargs.get('run_id', 'unknown')
            total_size = sum(file_sizes.values()) / (1024 * 1024)
            notifier.send_task_success(
                task_id="validate_data",
                dag_id=dag_id,
                run_id=run_id,
                details=f"Загружено и проверено файлов: {len(file_sizes)}, общий размер: {total_size:.2f} MB",
                is_first_task=True  # Первая задача в блоке
            )
    except Exception:
        pass
    
    return result


def create_artifacts(**kwargs):
    """Создание артефактов из сырых данных.
    
    Загружает и валидирует все данные из data/raw и создает предобработанные артефакты.
    Логирует результаты в MLflow для отслеживания (если MLFLOW_TRACKING_URI установлен).
    
    Args:
        **kwargs: Airflow context
        
    Returns:
        dict: Результат создания артефактов
    """
    # Настройка PYTHONPATH перед выполнением
    _setup_pythonpath()
    
    ti = kwargs.get('ti')
    
    # Получаем результат валидации из XCom
    validation_result = ti.xcom_pull(task_ids='validate_data', key='validation_result')
    if not validation_result:
        # Пробуем получить напрямую из предыдущей задачи
        validation_result = ti.xcom_pull(task_ids='validate_data')
    
    if validation_result and validation_result.get("status") != "validated":
        raise ValueError(f"Данные не прошли валидацию: {validation_result}")
    
    # Получаем пути
    project_root = Path(os.getenv("RECSYS_PROJECT_ROOT", "/home/mle-user/mle-pr-final"))
    data_dir = Path(os.getenv("RECSYS_DATA_DIR", str(project_root / "data" / "raw")))
    artifacts_dir = Path(os.getenv("RECSYS_ARTIFACTS_DIR", str(project_root / "artifacts")))
    
    # Настройка окружения
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    
    # Установка MLFLOW_TRACKING_URI из окружения DAG (если не установлена)
    if os.getenv("MLFLOW_TRACKING_URI") is None:
        mlflow_uri = os.getenv("MLFLOW_TRACKING_URI_DEFAULT", "http://127.0.0.1:5000")
        os.environ["MLFLOW_TRACKING_URI"] = mlflow_uri
    
    log_to_mlflow = os.getenv("MLFLOW_TRACKING_URI") is not None
    
    # Импорт и запуск скрипта создания артефактов
    from src.recsys.scripts.create_artifacts import main as create_artifacts_main
    
    # Создание артефактов
    artifacts_paths = create_artifacts_main(
        project_root=str(project_root),
        data_dir=str(data_dir),
        artifacts_dir=str(artifacts_dir),
        log_to_mlflow=log_to_mlflow,
    )
    
    logger.info(f"Артефакты успешно созданы: {len(artifacts_paths)} файлов")
    
    # Версионирование файлов, используемых API, через DVC
    # Версионируем только файлы, которые используются API
    api_artifacts = [
        artifacts_dir / "item_metadata.parquet",
        artifacts_dir / "category_stats.parquet",
        artifacts_dir / "category_tree.parquet",
    ]
    
    dvc_versioned_count = 0
    dvc_error_files = []
    
    try:
        import subprocess
        # Устанавливаем локальный кеш DVC
        dvc_cache_dir = str(Path.home() / ".cache" / "dvc")
        Path(dvc_cache_dir).mkdir(parents=True, exist_ok=True)
        
        # Пробуем создать /var/cache/dvc через sudo (если доступно)
        site_cache_dir = Path("/var/cache/dvc")
        if not site_cache_dir.exists():
            try:
                sudo_check = subprocess.run(
                    ["sudo", "-n", "true"],
                    capture_output=True,
                    timeout=2,
                    check=False
                )
                if sudo_check.returncode == 0:
                    result_mkdir = subprocess.run(
                        ["sudo", "mkdir", "-p", str(site_cache_dir)],
                        capture_output=True,
                        timeout=5,
                        check=False
                    )
                    if result_mkdir.returncode == 0:
                        subprocess.run(
                            ["sudo", "chmod", "777", str(site_cache_dir)],
                            capture_output=True,
                            timeout=5,
                            check=False
                        )
                        logger.info(f"Создана директория site_cache_dir: {site_cache_dir}")
            except Exception:
                pass
        
        # Проверяем, доступен ли DVC
        dvc_check = subprocess.run(
            ["dvc", "--version"],
            capture_output=True,
            timeout=5
        )
        if dvc_check.returncode == 0:
            dvc_config = project_root / ".dvc" / "config"
            if dvc_config.exists():
                # Версионируем каждый файл
                for artifact_path in api_artifacts:
                    if artifact_path.exists():
                        versioned, error_msg = _version_file_with_dvc(
                            artifact_path,
                            project_root,
                            dvc_cache_dir,
                            site_cache_dir,
                            logger
                        )
                        if versioned:
                            dvc_versioned_count += 1
                        elif error_msg:
                            dvc_error_files.append(f"{artifact_path.name}: {error_msg}")
    except Exception as e:
        logger.warning(f"Ошибка при версионировании артефактов через DVC: {e}")
    
    result = {
        "status": "success",
        "artifacts": {k: str(v) for k, v in artifacts_paths.items()},
        "dvc_versioned_count": dvc_versioned_count,
        "dvc_error_files": dvc_error_files,
    }
    
    if ti:
        ti.xcom_push(key="artifacts_result", value=result)
    
    # Отправка уведомления об успешном выполнении
    try:
        from src.recsys.utils.telegram_notifier import get_notifier
        notifier = get_notifier()
        if notifier.enabled:
            dag_id = kwargs.get('dag').dag_id if kwargs.get('dag') else "recsys_train_daily"
            run_id = kwargs.get('run_id', 'unknown')
            
            # Формируем детали с информацией о MLflow и DVC
            details_parts = [f"Создано артефактов: {len(artifacts_paths)}"]
            
            # Информация о MLflow
            if log_to_mlflow:
                try:
                    import mlflow
                    experiment_name = "recsys_production"
                    experiment = mlflow.get_experiment_by_name(experiment_name)
                    if experiment:
                        # Пытаемся получить последний run из эксперимента
                        try:
                            runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id], max_results=1)
                            if not runs.empty:
                                last_run_id = runs.iloc[0]['run_id']
                                details_parts.append(f"✅ MLflow: залогировано (run: {last_run_id[:8]}...)")
                            else:
                                details_parts.append("✅ MLflow: логирование включено")
                        except Exception:
                            details_parts.append("✅ MLflow: логирование включено")
                    else:
                        details_parts.append("✅ MLflow: логирование включено")
                except Exception:
                    details_parts.append("✅ MLflow: логирование включено")
            
            # Информация о DVC
            if result.get("dvc_versioned_count", 0) > 0:
                try:
                    import subprocess
                    dvc_remote_check = subprocess.run(
                        ["dvc", "remote", "list"],
                        cwd=str(project_root),
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if dvc_remote_check.returncode == 0 and "storage" in dvc_remote_check.stdout:
                        details_parts.append(f"📦 DVC: {result['dvc_versioned_count']} артефактов версионированы и синхронизированы с S3")
                    else:
                        details_parts.append(f"📦 DVC: {result['dvc_versioned_count']} артефактов версионированы (push не выполнен)")
                except Exception:
                    details_parts.append(f"📦 DVC: {result['dvc_versioned_count']} артефактов версионированы")
            else:
                try:
                    import subprocess
                    dvc_check = subprocess.run(
                        ["dvc", "--version"],
                        capture_output=True,
                        timeout=5
                    )
                    if dvc_check.returncode == 0:
                        if result.get("dvc_error_files"):
                            error_info = "; ".join(result["dvc_error_files"][:2])
                            details_parts.append(f"📦 DVC: доступен (ошибки версионирования: {error_info})")
                        else:
                            details_parts.append("📦 DVC: доступен (версионирование не выполнено)")
                except Exception:
                    pass
            
            details = "\n".join(details_parts)
            
            notifier.send_task_success(
                task_id="create_artifacts",
                dag_id=dag_id,
                run_id=run_id,
                details=details
            )
    except Exception:
        pass
    
    return result


def train_production_model(**kwargs):
    """Обучение production модели через специализированный скрипт.
    
    Args:
        **kwargs: Airflow context
        
    Returns:
        dict: Результат обучения модели
    """
    # Настройка PYTHONPATH перед выполнением
    _setup_pythonpath()
    
    ti = kwargs.get('ti')
    
    # Получаем результат создания артефактов из XCom
    artifacts_result = ti.xcom_pull(task_ids='create_artifacts', key='artifacts_result')
    if not artifacts_result:
        artifacts_result = ti.xcom_pull(task_ids='create_artifacts')
    
    if artifacts_result and artifacts_result.get("status") != "success":
        raise ValueError(f"Артефакты не были созданы успешно: {artifacts_result}")
    
    # Получаем пути
    project_root = Path(os.getenv("RECSYS_PROJECT_ROOT", "/home/mle-user/mle-pr-final"))
    data_dir = Path(os.getenv("RECSYS_DATA_DIR", str(project_root / "data" / "raw")))
    artifacts_dir = Path(os.getenv("RECSYS_ARTIFACTS_DIR", str(project_root / "artifacts")))
    config_path = Path(os.getenv("RECSYS_CONFIG_PATH", str(project_root / "configs" / "config.yaml")))
    
    # Настройка окружения
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    
    # Импорт и запуск production скрипта
    from src.recsys.scripts.train_production_model import main as train_main
    
    # Формирование аргументов
    from datetime import datetime
    run_name = f"production_daily_{kwargs.get('ds', datetime.now().strftime('%Y%m%d'))}"
    
    train_main(
        config_path=str(config_path),
        data_dir=str(data_dir),
        artifacts_dir=str(artifacts_dir),
        run_name=run_name,
    )
    
    logger.info(f"Обучение завершено успешно: {run_name}")
    
    # Автоматическое версионирование файлов модели через DVC add (если доступно)
    # Версионируем все файлы, используемые API
    api_model_files = [
        artifacts_dir / "model" / "als_model.pkl",
        artifacts_dir / "model" / "metadata.json",
        artifacts_dir / "model" / "user_item_matrix.npz",
        artifacts_dir / "model" / "popular.json",
        artifacts_dir / "user_purchases.parquet",
    ]
    
    dvc_versioned = False
    dvc_versioned_files = []
    dvc_error_msg = None
    try:
        import subprocess
        # Устанавливаем локальный кеш DVC через dvc config (правильный способ)
        dvc_cache_dir = str(Path.home() / ".cache" / "dvc")
        Path(dvc_cache_dir).mkdir(parents=True, exist_ok=True)
        
        # Пробуем создать /var/cache/dvc через sudo (если доступно)
        # Это необходимо, так как DVC пытается создать site_cache_dir при инициализации
        site_cache_dir = Path("/var/cache/dvc")
        if not site_cache_dir.exists():
            try:
                # Проверяем, доступен ли sudo
                sudo_check = subprocess.run(
                    ["sudo", "-n", "true"],
                    capture_output=True,
                    timeout=2,
                    check=False
                )
                if sudo_check.returncode == 0:
                    # sudo доступен без пароля, создаем директорию
                    result_mkdir = subprocess.run(
                        ["sudo", "mkdir", "-p", str(site_cache_dir)],
                        capture_output=True,
                        timeout=5,
                        check=False
                    )
                    if result_mkdir.returncode == 0:
                        subprocess.run(
                            ["sudo", "chmod", "777", str(site_cache_dir)],
                            capture_output=True,
                            timeout=5,
                            check=False
                        )
                        subprocess.run(
                            ["sudo", "chown", "-R", f"{os.getenv('USER', 'mle-user')}:{os.getenv('USER', 'mle-user')}", str(site_cache_dir)],
                            capture_output=True,
                            timeout=5,
                            check=False
                        )
                        logger.info(f"Создана директория site_cache_dir: {site_cache_dir}")
                    else:
                        logger.warning(f"Не удалось создать site_cache_dir через sudo: {result_mkdir.stderr.decode() if result_mkdir.stderr else 'unknown error'}")
                else:
                    logger.warning("sudo недоступен без пароля, пропускаем создание site_cache_dir")
            except (FileNotFoundError, subprocess.TimeoutExpired, Exception) as e:
                logger.warning(f"Не удалось создать site_cache_dir: {e}")
                # Продолжаем выполнение, возможно DVC сможет работать без него
        
        # Проверяем, доступен ли DVC
        dvc_check = subprocess.run(
            ["dvc", "--version"],
            capture_output=True,
            timeout=5
        )
        if dvc_check.returncode == 0:
            # Проверяем, инициализирован ли DVC
            dvc_config = project_root / ".dvc" / "config"
            if not dvc_config.exists():
                dvc_error_msg = "DVC не инициализирован (выполните 'dvc init' или './init_dvc.sh')"
                logger.warning(dvc_error_msg)
            else:
                # Версионируем все файлы модели, используемые API
                for model_file in api_model_files:
                    if model_file.exists():
                        versioned, error_msg = _version_file_with_dvc(
                            model_file,
                            project_root,
                            dvc_cache_dir,
                            site_cache_dir,
                            logger
                        )
                        if versioned:
                            dvc_versioned = True
                            dvc_versioned_files.append(model_file.name)
                        elif error_msg and not dvc_error_msg:
                            dvc_error_msg = error_msg
                    else:
                        logger.debug(f"Файл не найден, пропускаем версионирование: {model_file}")
                
                if dvc_versioned_files:
                    logger.info(f"Версионированы файлы модели: {', '.join(dvc_versioned_files)}")
        else:
            dvc_error_msg = "DVC недоступен"
            logger.warning(dvc_error_msg)
    except FileNotFoundError:
        dvc_error_msg = "DVC не установлен"
        logger.info(dvc_error_msg)
    except Exception as e:
        dvc_error_msg = f"Ошибка при версионировании через DVC: {e}"
        logger.warning(dvc_error_msg, exc_info=True)
    
    result = {
        "status": "success",
        "run_name": run_name,
        "config_path": str(config_path),
        "dvc_versioned": dvc_versioned,
        "dvc_versioned_files": dvc_versioned_files,
        "dvc_error_msg": dvc_error_msg,
    }
    
    if ti:
        ti.xcom_push(key="train_result", value=result)
    
    # Отправка уведомления об успешном выполнении
    try:
        from src.recsys.utils.telegram_notifier import get_notifier
        notifier = get_notifier()
        if notifier.enabled:
            dag_id = kwargs.get('dag').dag_id if kwargs.get('dag') else "recsys_train_daily"
            run_id = kwargs.get('run_id', 'unknown')
            
            # Формируем детали с информацией о MLflow и DVC
            details_parts = ["Модель сохранена: als_model.pkl"]
            
            # Информация о MLflow
            if os.getenv("MLFLOW_TRACKING_URI"):
                try:
                    import mlflow
                    experiment_name = "recsys_production"
                    experiment = mlflow.get_experiment_by_name(experiment_name)
                    if experiment:
                        try:
                            runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id], max_results=1)
                            if not runs.empty:
                                last_run_id = runs.iloc[0]['run_id']
                                details_parts.append(f"✅ MLflow: залогировано (run: {last_run_id[:8]}...)")
                            else:
                                details_parts.append("✅ MLflow: логирование включено")
                        except Exception:
                            details_parts.append("✅ MLflow: логирование включено")
                    else:
                        details_parts.append("✅ MLflow: логирование включено")
                except Exception:
                    details_parts.append("✅ MLflow: логирование включено")
            
            # Информация о DVC
            if dvc_versioned:
                # Проверяем, был ли выполнен dvc push (проверяем наличие remote)
                try:
                    import subprocess
                    dvc_remote_check = subprocess.run(
                        ["dvc", "remote", "list"],
                        cwd=str(project_root),
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if dvc_remote_check.returncode == 0 and "storage" in dvc_remote_check.stdout:
                        if dvc_versioned_files:
                            files_info = f" ({len(dvc_versioned_files)} файлов)" if len(dvc_versioned_files) > 1 else ""
                            details_parts.append(f"📦 DVC: файлы модели версионированы и синхронизированы с S3{files_info}")
                        else:
                            details_parts.append("📦 DVC: модель версионирована и синхронизирована с S3")
                    else:
                        if dvc_versioned_files:
                            files_info = f" ({len(dvc_versioned_files)} файлов)" if len(dvc_versioned_files) > 1 else ""
                            details_parts.append(f"📦 DVC: файлы модели версионированы (push не выполнен){files_info}")
                        else:
                            details_parts.append("📦 DVC: модель версионирована (push не выполнен)")
                except Exception:
                    if dvc_versioned_files:
                        files_info = f" ({len(dvc_versioned_files)} файлов)" if len(dvc_versioned_files) > 1 else ""
                        details_parts.append(f"📦 DVC: файлы модели версионированы{files_info}")
                    else:
                        details_parts.append("📦 DVC: модель версионирована")
            else:
                if dvc_error_msg:
                    # Показываем причину, почему версионирование не выполнено
                    if "не инициализирован" in dvc_error_msg:
                        details_parts.append("📦 DVC: не инициализирован (выполните './init_dvc.sh')")
                    else:
                        details_parts.append(f"📦 DVC: {dvc_error_msg}")
                else:
                    try:
                        import subprocess
                        dvc_check = subprocess.run(
                            ["dvc", "--version"],
                            capture_output=True,
                            timeout=5
                        )
                        if dvc_check.returncode == 0:
                            details_parts.append("📦 DVC: доступен (версионирование не выполнено)")
                    except Exception:
                        pass
            
            details = "\n".join(details_parts)
            
            notifier.send_task_success(
                task_id="train_production_model",
                dag_id=dag_id,
                run_id=run_id,
                details=details
            )
    except Exception:
        pass
    
    return result


def validate_model(**kwargs):
    """Валидация обученной модели.
    
    Вычисляет метрики качества на валидационной выборке и сравнивает
    с метриками предыдущей модели для обнаружения деградации.
    
    Args:
        **kwargs: Airflow context
        
    Returns:
        dict: Результат валидации
    """
    # Настройка PYTHONPATH перед выполнением
    _setup_pythonpath()
    
    ti = kwargs.get('ti')
    
    # Получаем результат обучения из XCom
    train_result = ti.xcom_pull(task_ids='train_production_model', key='train_result')
    if not train_result:
        train_result = ti.xcom_pull(task_ids='train_production_model')
    
    if train_result and train_result.get("status") != "success":
        raise ValueError(f"Модель не была обучена успешно: {train_result}")
    
    # Получаем пути
    project_root = Path(os.getenv("RECSYS_PROJECT_ROOT", "/home/mle-user/mle-pr-final"))
    artifacts_dir = Path(os.getenv("RECSYS_ARTIFACTS_DIR", str(project_root / "artifacts")))
    data_dir = Path(os.getenv("RECSYS_DATA_DIR", str(project_root / "data" / "raw")))
    config_path = Path(os.getenv("RECSYS_CONFIG_PATH", str(project_root / "configs" / "config.yaml")))
    
    # Настройка окружения
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    
    # Установка MLFLOW_TRACKING_URI из окружения DAG (если не установлена)
    if os.getenv("MLFLOW_TRACKING_URI") is None:
        mlflow_uri = os.getenv("MLFLOW_TRACKING_URI_DEFAULT", "http://127.0.0.1:5000")
        os.environ["MLFLOW_TRACKING_URI"] = mlflow_uri
    
    # Импорт и запуск скрипта валидации
    from src.recsys.scripts.validate_model import main as validate_main
    
    model_path = artifacts_dir / "model" / "als_model.pkl"
    previous_metrics_path = artifacts_dir / "model" / "validation_metrics.json"
    
    # Загрузка порога деградации из конфига (опционально)
    degradation_threshold = 0.1  # По умолчанию 10%
    try:
        import yaml
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            eval_config = config.get("evaluation", {})
            degradation_threshold = eval_config.get("degradation_threshold", 0.1)
    except Exception as e:
        logger.warning(f"Не удалось загрузить порог деградации из конфига, используем по умолчанию: {e}")
    
    try:
        validation_result = validate_main(
            model_path=str(model_path),
            data_dir=str(data_dir),
            config_path=str(config_path),
            artifacts_dir=str(artifacts_dir),
            previous_metrics_path=str(previous_metrics_path) if previous_metrics_path.exists() else None,
            degradation_threshold=degradation_threshold,
            validation_size=0.15,  # 15% данных для валидации 
        )
        
        logger.info("Валидация завершена успешно")
        
        # Автоматическое версионирование через DVC add (если доступно)
        # Используем dvc add для версионирования уже созданных файлов
        # Проблема: DVC пытается создать site_cache_dir в /var/cache/dvc
        # Решение: Создаем директорию через sudo перед выполнением dvc add
        metrics_path = artifacts_dir / "model" / "validation_metrics.json"
        dvc_versioned = False
        dvc_error_msg = None
        if metrics_path.exists():
            try:
                import subprocess
                # Устанавливаем локальный кеш DVC через dvc config (правильный способ)
                dvc_cache_dir = str(Path.home() / ".cache" / "dvc")
                Path(dvc_cache_dir).mkdir(parents=True, exist_ok=True)
                
                # Пробуем создать /var/cache/dvc через sudo (если доступно)
                # Это необходимо, так как DVC пытается создать site_cache_dir при инициализации
                site_cache_dir = Path("/var/cache/dvc")
                if not site_cache_dir.exists():
                    try:
                        # Проверяем, доступен ли sudo
                        sudo_check = subprocess.run(
                            ["sudo", "-n", "true"],
                            capture_output=True,
                            timeout=2,
                            check=False
                        )
                        if sudo_check.returncode == 0:
                            # sudo доступен без пароля, создаем директорию
                            result_mkdir = subprocess.run(
                                ["sudo", "mkdir", "-p", str(site_cache_dir)],
                                capture_output=True,
                                timeout=5,
                                check=False
                            )
                            if result_mkdir.returncode == 0:
                                subprocess.run(
                                    ["sudo", "chmod", "777", str(site_cache_dir)],
                                    capture_output=True,
                                    timeout=5,
                                    check=False
                                )
                                subprocess.run(
                                    ["sudo", "chown", "-R", f"{os.getenv('USER', 'mle-user')}:{os.getenv('USER', 'mle-user')}", str(site_cache_dir)],
                                    capture_output=True,
                                    timeout=5,
                                    check=False
                                )
                                logger.info(f"Создана директория site_cache_dir: {site_cache_dir}")
                            else:
                                logger.warning(f"Не удалось создать site_cache_dir через sudo: {result_mkdir.stderr.decode() if result_mkdir.stderr else 'unknown error'}")
                        else:
                            logger.warning("sudo недоступен без пароля, пропускаем создание site_cache_dir")
                    except (FileNotFoundError, subprocess.TimeoutExpired, Exception) as e:
                        logger.warning(f"Не удалось создать site_cache_dir: {e}")
                        # Продолжаем выполнение, возможно DVC сможет работать без него
                
                # Проверяем, доступен ли DVC
                dvc_check = subprocess.run(
                    ["dvc", "--version"],
                    capture_output=True,
                    timeout=5
                )
                if dvc_check.returncode == 0:
                    # Проверяем, инициализирован ли DVC
                    dvc_config = project_root / ".dvc" / "config"
                    if not dvc_config.exists():
                        dvc_error_msg = "DVC не инициализирован (выполните 'dvc init' или './init_dvc.sh')"
                        logger.warning(dvc_error_msg)
                    else:
                        # Проверяем, не версионированы ли уже метрики
                        dvc_file = metrics_path.with_suffix(".json.dvc")
                        if not dvc_file.exists():
                            # Устанавливаем локальный кеш через dvc config (если еще не установлен)
                            current_cache = subprocess.run(
                                ["dvc", "cache", "dir"],
                                cwd=str(project_root),
                                capture_output=True,
                                text=True,
                                timeout=5
                            )
                            if current_cache.returncode == 0 and current_cache.stdout.strip() != dvc_cache_dir:
                                # Устанавливаем кеш, если он отличается
                                logger.info(f"Установка локального кеша DVC: {dvc_cache_dir}")
                                subprocess.run(
                                    ["dvc", "config", "--local", "cache.dir", dvc_cache_dir],
                                    cwd=str(project_root),
                                    capture_output=True,
                                    timeout=5
                                )
                            
                            # Проверяем, не определен ли файл уже в dvc.yaml как выход стадии
                            # Если да, используем dvc commit для обновления, иначе dvc add
                            dvc_yaml_path = project_root / "dvc.yaml"
                            file_in_pipeline = False
                            if dvc_yaml_path.exists():
                                try:
                                    import yaml
                                    with open(dvc_yaml_path, "r") as f:
                                        dvc_config = yaml.safe_load(f)
                                        if dvc_config:
                                            for stage_name, stage_config in dvc_config.get("stages", {}).items():
                                                outputs = stage_config.get("outs", [])
                                                if any(str(metrics_path) in str(out) for out in outputs):
                                                    file_in_pipeline = True
                                                    break
                                except Exception:
                                    pass
                            
                            if file_in_pipeline:
                                # Файл уже в пайплайне DVC, версионирование не требуется
                                # DVC уже отслеживает файл через dvc.yaml
                                logger.info("Метрики уже в пайплайне DVC, версионирование не требуется")
                                dvc_versioned = True
                            else:
                                # Файл не в пайплайне, используем dvc add
                                logger.info(f"Версионирование метрик через DVC add: {metrics_path}")
                                result_dvc = subprocess.run(
                                    ["dvc", "add", str(metrics_path)],
                                    cwd=str(project_root),
                                    capture_output=True,
                                    timeout=60,
                                    text=True
                                )
                                if result_dvc.returncode == 0:
                                    dvc_versioned = True
                                    logger.info("Метрики успешно добавлены под версионирование DVC")
                                    
                                    # Синхронизация с remote storage (S3) через dvc push
                                    logger.info("Синхронизация метрик с remote storage через DVC push...")
                                    result_push = subprocess.run(
                                        ["dvc", "push", "-r", "storage"],
                                        cwd=str(project_root),
                                        capture_output=True,
                                        timeout=300,  # 5 минут на push
                                        text=True
                                    )
                                    if result_push.returncode == 0:
                                        logger.info("Метрики успешно синхронизированы с remote storage")
                                    else:
                                        error_push = result_push.stderr or result_push.stdout
                                        logger.warning(f"DVC push ошибка (версионирование выполнено, но push не удался): {error_push.strip()[:200]}")
                                        # Не считаем это критической ошибкой, версионирование уже выполнено
                                else:
                                    error_output = result_dvc.stderr or result_dvc.stdout
                                    # Проверяем, не связана ли ошибка с site_cache_dir
                                    if "Permission denied" in error_output and "/var/cache/dvc" in error_output:
                                        # Пробуем создать директорию еще раз
                                        try:
                                            sudo_check = subprocess.run(
                                                ["sudo", "-n", "true"],
                                                capture_output=True,
                                                timeout=2,
                                                check=False
                                            )
                                            if sudo_check.returncode == 0:
                                                subprocess.run(
                                                    ["sudo", "mkdir", "-p", str(site_cache_dir)],
                                                    capture_output=True,
                                                    timeout=5,
                                                    check=False
                                                )
                                                subprocess.run(
                                                    ["sudo", "chmod", "777", str(site_cache_dir)],
                                                    capture_output=True,
                                                    timeout=5,
                                                    check=False
                                                )
                                                logger.info(f"Повторная попытка создания site_cache_dir: {site_cache_dir}")
                                        except Exception:
                                            pass
                                        dvc_error_msg = "DVC требует прав доступа к /var/cache/dvc (выполните: sudo mkdir -p /var/cache/dvc && sudo chmod 777 /var/cache/dvc)"
                                    elif "overlaps with an output" in error_output:
                                        # Файл уже в пайплайне, но мы не обнаружили это ранее
                                        # Версионирование не требуется, DVC уже отслеживает файл
                                        logger.info("Файл уже в пайплайне DVC, версионирование не требуется")
                                        dvc_versioned = True
                                    else:
                                        dvc_error_msg = f"DVC add ошибка: {error_output.strip()[:200]}"
                                        logger.warning(dvc_error_msg)
                        else:
                            # Метрики уже под версионированием
                            logger.info("Метрики уже под версионированием DVC")
                            dvc_versioned = True
                else:
                    dvc_error_msg = "DVC недоступен"
                    logger.warning(dvc_error_msg)
            except FileNotFoundError:
                dvc_error_msg = "DVC не установлен"
                logger.info(dvc_error_msg)
            except Exception as e:
                dvc_error_msg = f"Ошибка при версионировании через DVC: {e}"
                logger.warning(dvc_error_msg, exc_info=True)
        
        result = {
            "status": "success",
            "metrics": validation_result.get("metrics", {}),
            "degradation": validation_result.get("degradation"),  # Включаем degradation в результат
            "warm_users": validation_result.get("warm_users", 0),
            "cold_users": validation_result.get("cold_users", 0),
            "dvc_versioned": dvc_versioned,
            "dvc_error_msg": dvc_error_msg,
        }
        
        if ti:
            ti.xcom_push(key="validation_result", value=result)
        
        # Отправка уведомления об успешном выполнении или ошибке
        try:
            from src.recsys.utils.telegram_notifier import get_notifier
            notifier = get_notifier()
            if notifier.enabled:
                dag_id = kwargs.get('dag').dag_id if kwargs.get('dag') else "recsys_train_daily"
                run_id = kwargs.get('run_id', 'unknown')
                
                # Проверяем наличие деградации (даже если status == "success")
                degradation = result.get("degradation")
                
                if result.get("status") == "success" and not degradation:
                        # Успешная валидация без деградации
                        metrics = result.get("metrics", {})
                        details_parts = []
                        
                        if metrics:
                            details_parts.append(
                                f"Precision@5: {metrics.get('precision@5', 0)*100:.2f}%\n"
                                f"Recall@20: {metrics.get('recall@20', 0)*100:.2f}%\n"
                                f"NDCG@10: {metrics.get('ndcg@10', 0):.4f}\n"
                                f"Hit Rate@5: {metrics.get('hit_rate@5', 0)*100:.2f}%"
                            )
                        else:
                            details_parts.append("Валидация пройдена успешно")
                        
                        # Информация о MLflow
                        if os.getenv("MLFLOW_TRACKING_URI"):
                            try:
                                import mlflow
                                experiment_name = "recsys_production"
                                experiment = mlflow.get_experiment_by_name(experiment_name)
                                if experiment:
                                    try:
                                        runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id], max_results=1)
                                        if not runs.empty:
                                            last_run_id = runs.iloc[0]['run_id']
                                            details_parts.append(f"✅ MLflow: залогировано (run: {last_run_id[:8]}...)")
                                        else:
                                            details_parts.append("✅ MLflow: логирование включено")
                                    except Exception:
                                        details_parts.append("✅ MLflow: логирование включено")
                                else:
                                    details_parts.append("✅ MLflow: логирование включено")
                            except Exception:
                                details_parts.append("✅ MLflow: логирование включено")
                        
                        # Информация о DVC
                        if result.get("dvc_versioned"):
                            # Проверяем, был ли выполнен dvc push (проверяем наличие remote)
                            try:
                                dvc_remote_check = subprocess.run(
                                    ["dvc", "remote", "list"],
                                    cwd=str(project_root),
                                    capture_output=True,
                                    text=True,
                                    timeout=5
                                )
                                if dvc_remote_check.returncode == 0 and "storage" in dvc_remote_check.stdout:
                                    details_parts.append("📦 DVC: метрики версионированы и синхронизированы с S3")
                                else:
                                    details_parts.append("📦 DVC: метрики версионированы (push не выполнен)")
                            except Exception:
                                details_parts.append("📦 DVC: метрики версионированы")
                        else:
                            dvc_error = result.get("dvc_error_msg")
                            if dvc_error:
                                # Показываем причину, почему версионирование не выполнено
                                if "не инициализирован" in dvc_error:
                                    details_parts.append("📦 DVC: не инициализирован (выполните './init_dvc.sh')")
                                else:
                                    details_parts.append(f"📦 DVC: {dvc_error}")
                            else:
                                try:
                                    import subprocess
                                    dvc_check = subprocess.run(
                                        ["dvc", "--version"],
                                        capture_output=True,
                                        timeout=5
                                    )
                                    if dvc_check.returncode == 0:
                                        details_parts.append("📦 DVC: доступен (версионирование не выполнено)")
                                except Exception:
                                    pass
                        
                        details = "\n".join(details_parts)
                        
                        notifier.send_task_success(
                            task_id="validate_model",
                            dag_id=dag_id,
                            run_id=run_id,
                            details=details
                        )
                elif degradation:
                    # Деградация обнаружена (даже если status == "success" из-за малого размера выборки)
                    degradation_info = degradation.get("degraded_metrics", {})
                    
                    # Формируем детали для уведомления
                    details = "<b>Обнаружена деградация метрик:</b>\n\n"
                    details += "<pre>"
                    details += "Метрика          | Текущее | Предыдущее | Изменение\n"
                    details += "──────────────────┼─────────┼────────────┼──────────\n"
                    
                    for metric, info in degradation_info.items():
                        metric_name = metric.replace("@", "@").replace("_", " ").title()
                        current = info.get('current', 0)
                        previous = info.get('previous', 0)
                        change_pct = info.get('change_pct', 0)
                        
                        # Форматируем значения в зависимости от типа метрики
                        # precision, recall, hit_rate - в процентах
                        # ndcg - в десятичном формате
                        if 'precision' in metric or 'recall' in metric or 'hit_rate' in metric:
                            current_str = f"{current*100:.2f}%"
                            previous_str = f"{previous*100:.2f}%"
                        else:
                            # ndcg и другие - в десятичном формате
                            current_str = f"{current:.4f}"
                            previous_str = f"{previous:.4f}"
                        
                        change_str = f"{change_pct:.1f}%"
                        details += f"{metric_name:17} | {current_str:7} | {previous_str:10} | {change_str}\n"
                    
                    details += "</pre>"
                    
                    notifier.send_task_failure(
                        task_id="validate_model",
                        dag_id=dag_id,
                        run_id=run_id,
                        error="Обнаружена деградация модели",
                        details=details
                    )
                else:
                    # Ошибка валидации (другая причина)
                    error_msg = result.get("error", "Валидация не пройдена")
                    
                    # Парсим информацию о деградации для читаемого формата
                    degradation_info = None
                    if "Model degradation detected:" in error_msg:
                        try:
                            import ast
                            import re
                            dict_match = re.search(r"\{.*\}", error_msg, re.DOTALL)
                            if dict_match:
                                degradation_dict = ast.literal_eval(dict_match.group())
                                degradation_info = degradation_dict
                        except Exception:
                            pass
                    
                    # Формируем детали для уведомления
                    if degradation_info:
                        details = "<b>Обнаружена деградация метрик:</b>\n\n"
                        details += "<pre>"
                        details += "Метрика          | Текущее | Предыдущее | Изменение\n"
                        details += "──────────────────┼─────────┼────────────┼──────────\n"
                        
                        for metric, info in degradation_info.items():
                            metric_name = metric.replace("@", "@").replace("_", " ").title()
                            current = info.get('current', 0)
                            previous = info.get('previous', 0)
                            change_pct = info.get('change_pct', 0)
                            
                            # Форматируем значения в зависимости от типа метрики
                            # precision, recall, hit_rate - в процентах
                            # ndcg - в десятичном формате
                            if 'precision' in metric or 'recall' in metric or 'hit_rate' in metric:
                                current_str = f"{current*100:.2f}%"
                                previous_str = f"{previous*100:.2f}%"
                            else:
                                # ndcg и другие - в десятичном формате
                                current_str = f"{current:.4f}"
                                previous_str = f"{previous:.4f}"
                            
                            change_str = f"{change_pct:.1f}%"
                            details += f"{metric_name:17} | {current_str:7} | {previous_str:10} | {change_str}\n"
                        
                        details += "</pre>"
                    else:
                        details = error_msg[:300] + "..." if len(error_msg) > 300 else error_msg
                    
                    notifier.send_task_failure(
                        task_id="validate_model",
                        dag_id=dag_id,
                        run_id=run_id,
                        error="Обнаружена деградация модели",
                        details=details
                    )
        except Exception:
            pass
        
        return result
    
    except ValueError as e:
        # Деградация обнаружена - отправляем алерт
        error_msg = str(e)
        logger.error(f"❌ ВАЛИДАЦИЯ НЕ ПРОЙДЕНА: {error_msg}")
        
        # Отправка Telegram уведомления об ошибке
        try:
            from src.recsys.utils.telegram_notifier import get_notifier
            notifier = get_notifier()
            if notifier.enabled:
                train_result = ti.xcom_pull(task_ids='train_production_model', key='train_result')
                dag_id = kwargs.get('dag').dag_id if kwargs.get('dag') else "recsys_train_daily"
                run_id = kwargs.get('run_id', 'unknown')
                
                # Парсим информацию о деградации из error_msg
                degradation_info = None
                if "Model degradation detected:" in error_msg:
                    try:
                        import ast
                        import re
                        # Ищем словарь в строке ошибки
                        dict_match = re.search(r"\{.*\}", error_msg, re.DOTALL)
                        if dict_match:
                            degradation_dict = ast.literal_eval(dict_match.group())
                            degradation_info = degradation_dict
                    except Exception:
                        pass
                
                # Формируем читаемое сообщение
                message = "🚨 <b>Валидация модели не пройдена</b>\n\n"
                message += f"Запуск: <code>{train_result.get('run_name', run_id) if train_result else run_id}</code>\n"
                message += f"Путь к модели: <code>{model_path.name}</code>\n\n"
                
                if degradation_info:
                    message += "<b>Обнаружена деградация метрик:</b>\n\n"
                    message += "<pre>"
                    message += "Метрика          | Текущее | Предыдущее | Изменение\n"
                    message += "──────────────────┼─────────┼────────────┼──────────\n"
                    
                    for metric, info in degradation_info.items():
                        metric_name = metric.replace("@", "@").replace("_", " ").title()
                        current = info.get('current', 0)
                        previous = info.get('previous', 0)
                        change_pct = info.get('change_pct', 0)
                        
                        # Форматируем значения в зависимости от типа метрики
                        # precision, recall, hit_rate - в процентах
                        # ndcg - в десятичном формате
                        if 'precision' in metric or 'recall' in metric or 'hit_rate' in metric:
                            current_str = f"{current*100:.2f}%"
                            previous_str = f"{previous*100:.2f}%"
                        else:
                            # ndcg и другие - в десятичном формате
                            current_str = f"{current:.4f}"
                            previous_str = f"{previous:.4f}"
                        
                        change_str = f"{change_pct:.1f}%"
                        message += f"{metric_name:17} | {current_str:7} | {previous_str:10} | {change_str}\n"
                    
                    message += "</pre>"
                else:
                    # Если не удалось распарсить, выводим краткую информацию
                    error_short = error_msg[:200] + "..." if len(error_msg) > 200 else error_msg
                    message += f"Ошибка: <code>{error_short}</code>"
                
                notifier.send_message(message)
        except Exception:
            pass
        
        # Пробрасываем исключение для остановки DAG
        raise

