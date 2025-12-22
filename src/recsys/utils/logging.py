from __future__ import annotations

import logging
import os
import datetime
from pathlib import Path
from typing import Optional

import polars as pl


def get_log_file_path(
    script_name: str,
    logs_dir: Path,
    timestamp: Optional[datetime.datetime] = None
) -> Path:
    """Создать путь к файлу лога с timestamp.
    
    Args:
        script_name: Имя скрипта (без расширения)
        logs_dir: Директория для логов
        timestamp: Временная метка (если None, используется текущее время)
    
    Returns:
        Path к файлу лога
    """
    if timestamp is None:
        timestamp = datetime.datetime.now()
    
    # Формат: имя_скрипта_YYYY-MM-DD_HH-MM-SS.log
    timestamp_str = timestamp.strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"{script_name}_{timestamp_str}.log"
    
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir / log_filename


def setup_notebook_logging(
    logger: logging.Logger,
    log_file_path: Path,
    log_level: str = "INFO",
    log_to_console: bool = True,
    log_to_file: bool = True,
) -> None:
    """Настраивает логирование для Jupyter notebooks.
    
    Добавляет handlers для консоли и файла, если их еще нет.
    Используется в notebooks для единообразной настройки логирования.
    
    Args:
        logger: Логгер для настройки
        log_file_path: Путь к файлу лога (с timestamp)
        log_level: Уровень логирования (INFO, DEBUG, WARNING, ERROR)
        log_to_console: Логировать в консоль (по умолчанию True)
        log_to_file: Логировать в файл (по умолчанию True)
    """
    # Если handlers уже есть, не добавляем повторно
    if logger.handlers:
        return
    
    # Устанавливаем уровень логирования
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    
    # Форматтер
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Handler для консоли
    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # Handler для файла
    if log_to_file:
        try:
            file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)  # В файл пишем всё
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except (IOError, OSError) as e:
            # Если файл заблокирован другим процессом, продолжаем без файлового лога
            logger.warning(f"Не удалось открыть файл лога (возможно, открыт другим процессом): {e}")
    
    logger.propagate = False


def get_logger(
    name: str,
    script_name: Optional[str] = None,
    logs_dir: Optional[Path] = None,
    log_to_file: bool = True,
    log_to_console: bool = False
) -> logging.Logger:
    """Получить настроенный логгер для модуля.
    
    Args:
        name: Имя логгера (обычно __name__)
        script_name: Имя скрипта для создания файла лога (если None, используется name)
        logs_dir: Директория для логов (если None, используется logs/ в project_root)
        log_to_file: Логировать в файл (по умолчанию True)
        log_to_console: Логировать в консоль (по умолчанию False)
    
    Returns:
        Настроенный логгер
    """
    logger = logging.getLogger(name)
    
    # Если логгер уже настроен, возвращаем его
    if logger.handlers:
        return logger
    
    # Настройка уровня логирования
    log_level = os.getenv("PROCESSING_LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    
    # Форматтер
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # File handler (только в файл, если указано)
    if log_to_file:
        if script_name is None:
            script_name = name.split('.')[-1]
        if logs_dir is None:
            # Определяем project_root
            current_file = Path(__file__)
            project_root = current_file.parent.parent.parent
            logs_dir = project_root / "logs"
        
        log_file = get_log_file_path(script_name, logs_dir)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except (IOError, OSError) as e:
            # Если файл заблокирован другим процессом, продолжаем без файлового лога
            logger.warning(f"Не удалось открыть файл лога (возможно, открыт другим процессом): {e}")
    
    # Console handler (только если указано)
    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, log_level, logging.INFO))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    logger.propagate = False
    
    return logger


def setup_logging(level: Optional[str] = None) -> None:
    """Настраивает корневое логирование один раз для CLI скриптов.
    
    Args:
        level: Уровень логирования (если None, берётся из переменной окружения LOG_LEVEL или INFO)
    """
    chosen = (level or os.getenv("LOG_LEVEL") or "INFO").upper()
    logging.basicConfig(
        level=chosen,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def configure_polars_logging() -> None:
    """Настройка логирования и отображения для Polars."""
    pl.Config.set_fmt_str_lengths(100)
    pl.Config.set_tbl_rows(20)
    pl.Config.set_tbl_cols(20)


