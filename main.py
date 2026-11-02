from parallel_tasks import parallel

def main() -> None:
    lab = parallel()
    
    lab.ping_comparison()
    lab.producer_consumer()
    lab.async_generator()

if __name__ == "__main__":
    main()