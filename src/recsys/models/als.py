from __future__ import annotations

import logging
import os
import sys
from contextlib import contextmanager

import scipy.sparse as sp

logger = logging.getLogger(__name__)


@contextmanager
def suppress_tqdm_output():
    """Подавляет вывод tqdm для предотвращения Broken pipe ошибок."""
    # Отключаем tqdm через переменную окружения
    old_env = os.environ.get("IMPLICIT_DISABLE_TQDM", None)
    os.environ["IMPLICIT_DISABLE_TQDM"] = "1"
    
    # Сохраняем stderr
    old_stderr = sys.stderr
    
    try:
        # Перенаправляем stderr в /dev/null для tqdm
        with open(os.devnull, "w") as devnull:
            sys.stderr = devnull
            yield
    finally:
        # Восстанавливаем stderr
        sys.stderr = old_stderr
        
        # Восстанавливаем переменную окружения
        if old_env is None:
            os.environ.pop("IMPLICIT_DISABLE_TQDM", None)
        else:
            os.environ["IMPLICIT_DISABLE_TQDM"] = old_env


def train_als(
    user_item: sp.csr_matrix,
    factors: int,
    iterations: int,
    regularization: float,
    alpha: float,
    random_state: int = 42,
    num_threads: int | None = None,
):
    """Обучает implicit ALS на CSR матрице пользователь-товар.
    
    Args:
        user_item: Разреженная CSR матрица взаимодействий пользователь-товар
        factors: Количество факторов в разложении матрицы
        iterations: Количество итераций обучения
        regularization: Коэффициент регуляризации
        alpha: Параметр масштабирования неявных взаимодействий
        random_state: Seed для воспроизводимости
        num_threads: Количество потоков (None или 0 = автоматически)
    
    Returns:
        Обученная модель implicit ALS
    """
    import implicit

    # Преобразуем None в 0 для num_threads (требование библиотеки implicit)
    threads = num_threads if num_threads is not None else 0
    model = implicit.als.AlternatingLeastSquares(
        factors=factors,
        iterations=iterations,
        regularization=regularization,
        alpha=alpha,
        random_state=random_state,
        num_threads=threads,
    )
    logger.info(
        "Train ALS: factors=%s iters=%s reg=%s alpha=%s",
        factors,
        iterations,
        regularization,
        alpha,
    )
    
    # Подавляем tqdm вывод для предотвращения Broken pipe
    with suppress_tqdm_output():
        model.fit(user_item)
    
    return model


