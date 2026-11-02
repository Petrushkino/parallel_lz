import subprocess
import time
import concurrent.futures
import multiprocessing
import asyncio
import random
import platform

class parallel:
    def __init__(self):
        self.ips = [
            "8.8.8.8", "8.8.4.4", "1.1.1.1", "1.0.0.1", 
            "9.9.9.9", "149.112.112.112", "208.67.222.222", 
            "208.67.220.220", "8.26.56.26", "8.20.247.20"
        ]
        self.ping_param = '-n' if platform.system().lower() == 'windows' else '-c'
    
    def ping(self, ip: str) -> None:
        result = subprocess.run(["ping", self.ping_param, "1", ip], capture_output=True)
        print(f"{ip} is {'up' if result.returncode == 0 else 'down'}")    
        
    def ping_comparison(self) -> None:
        print("Пинг серверов")
        
        start_seq = time.time()
        print("[Последовательный запуск]")
        for ip in self.ips:
            self.ping(ip)
        time_seq = time.time() - start_seq

        start_par = time.time()
        print("\n[Параллельный запуск]")
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(self.ping, self.ips)
        time_par = time.time() - start_par

        print(f"\nВремя последовательного выполнения: {time_seq:.2f} сек")
        print(f"Время параллельного выполнения (потоки): {time_par:.2f} сек")
        print(f"Разница: параллельный метод быстрее на {time_seq - time_par:.2f} сек\n")
        
    @staticmethod
    def _producer(q: multiprocessing.Queue) -> None:
        for i in range(1, 16):  
            item = f"Пакет данных {i}"
            q.put(item)
            print(f"[Производитель] Сгенерирован и добавлен: {item}")
            time.sleep(0.2)
        q.put(None) 

    @staticmethod
    def _consumer(q: multiprocessing.Queue) -> None:
        while True:
            item = q.get()
            if item is None: 
                print("[Потребитель] Работа завершена.")
                break
            print(f"[Потребитель] Обработан: {item}")
            time.sleep(0.5) 

    def producer_consumer(self) -> None:
        print("Производитель-Потребитель")
        queue = multiprocessing.Queue(maxsize=10)
        
        producer_proc = multiprocessing.Process(target=self._producer, args=(queue,))
        consumer_proc = multiprocessing.Process(target=self._consumer, args=(queue,))
        
        producer_proc.start()
        consumer_proc.start()
        
        producer_proc.join()
        consumer_proc.join()
        print()
        
    async def _async_gen(self):
        start_time = time.time()
        current = 1
        
        while time.time() - start_time < 15 and current <= 1_000_000:
            yield current
            current += random.randint(1000, 50000) 
            await asyncio.sleep(0.1)

    async def _run_async_task(self) -> None:
        print("Асинхронный генератор")
        async for number in self._async_gen():
            print(f"Сгенерировано число: {number}")

    def async_generator(self) -> None:
        asyncio.run(self._run_async_task())