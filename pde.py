# coding: utf-8
"""マルチプロセスで実行処理の汎用モジュール
"""
import os
from concurrent import futures
from tqdm import tqdm


class ProcessDivisionier:
    """マルチプロセスでの実行処理クラス

    分散可能な処理をマルチプロセスで実行後、結果をリストに集計する.
    単純な集計処理などで、ループ要素が多いものなどを高速化する目的
    """
    def __init__(self, func):
        """コンストラクタ
        Args:
            func (FUNC): 分割して実行する関数
        """
        self.__f = func
        self.__all_args = []
        self.__num_of_process = 1

    def show_args(self):
        """登録済の引数リストを確認する
        Args:
            None
        Returns:
            (list): 引数リスト
        """
        return self.__all_args

    def add_args(self, *args):
        """プロセスの実行に使う引数を追加する
        Args:
            *args (any in tuple): 引数のタプル
        Returns:
            None
        """
        self.__all_args.append(args)
        self.__num_of_process = len(self.__all_args)

    def set_allargs(self, all_args, mode='w'):
        """プロセスの実行に使う引数をまとめて設定する
        Args:
            *all_args (tuple in tuple): 引数のタプルのタプル
            mode (str): 元の引数リストに追加するか上書きするか
                'w': 上書き、'a': 追加
        Returns:
            None
        """
        # 上書きモードだったらクリア
        if mode == 'w':
            self.__all_args = []
        # 内容確認
        if any((not isinstance(args, tuple) for args in all_args)):
            raise TypeError("与える引数はタプルでまとめてください")
        self.__all_args.extend(list(all_args))
        self.__num_of_process = len(self.__all_args)

    def set_processes(self, num_of_process):
        """runner実行時のプロセス数を設定する
        Args:
            num_of_process (int): 実行プロセス数
        Returns:
            None
        """
        self.__num_of_process = num_of_process
        self.__all_args = [tuple()] * self.__num_of_process

    def run(self, workers=os.cpu_count(), all_args=None):
        """マルチプロセスでの実行
        Args:
            workers (int): 並列実行プロセスの数
            all_args (tuple in tuple or list): 実行プロセス分の引数のタプル.
                Noneを指定すると事前設定のプロパティから引数を使う.
                デフォルトはNone.
        Returns:
            (list): 各プロセスの戻り値を格納したリスト.
                戻り値がなくてもNoneのリストが帰る.
        """
        # 引数と実行プロセス数を整理する
        if all_args is not None:
            self.set_allargs(all_args)
        # 指定した並行数のタスクにプロセスを割り当てる
        with futures.ProcessPoolExecutor(max_workers=workers) as e:
            # プロセスの登録と実行
            fs = [e.submit(self.__f, *args) for args in self.__all_args]
            # 進捗表示と結果集計
            pbar = tqdm(total=len(fs))
            results = []
            def update(future):
                """プロセス完了時のコールバック
                """
                nonlocal pbar
                pbar.update(1)
                # 例外発生してたらメインプロセスを止める
                exception = future.exception()
                if exception is not None:
                    raise exception
                # 結果を集計
                results.append(future.result())
            # コールバック設定して完了待ち
            [f.add_done_callback(update) for f in fs]
            futures.wait(fs)
        return results
