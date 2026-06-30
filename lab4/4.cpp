#include <mpi.h>
#include <iostream>
#include <queue>
#include <vector>
#include <pthread.h>
#include <unistd.h> // Для sleep

// ТЕГИ СООБЩЕНИЙ MPI
#define TAG_REQUEST_JOB 1
#define TAG_RESPONSE_JOB 2

// Глобальные переменные синхронизации
pthread_mutex_t mutex_queue;       // Защита очереди
pthread_cond_t cond_need_work;     // Сигнал "Нужна работа"
bool stop_system = false;          // Флаг завершения работы

struct Job {
    int id;
    int jobSimulationTime;
    Job(int _id, int jst) : id(_id), jobSimulationTime(jst) {}
    Job() : id(-1), jobSimulationTime(0) {}

    void process() {
        // Симуляция работы
        usleep(jobSimulationTime * 1000); // usleep принимает микросекунды
    }
};

std::queue<Job> jobQueue;
int rank_id, comm_size;

//  WORKER: Обрабатывает задачи 
void* worker(void* args) {
    while (true) {
        Job job;
        bool haveJob = false;

        pthread_mutex_lock(&mutex_queue);

        
        if (jobQueue.size() < 2 && !stop_system) {
            // Если задач мало (< 2), будим Fetcher'а
            pthread_cond_signal(&cond_need_work);
        }

        if (!jobQueue.empty()) {
            job = jobQueue.front();
            jobQueue.pop();
            haveJob = true;
        }
        else if (stop_system) {
            pthread_mutex_unlock(&mutex_queue);
            break; // Выход из цикла, если работы нет и система останавливается
        }

        pthread_mutex_unlock(&mutex_queue);

        if (haveJob) {
            // printf("[%d] Worker processing job %d\n", rank_id, job.id);
            job.process();
        }
        else {
            // Если работы нет, чуть-чуть ждем, чтобы не спамить 
            usleep(1000);
        }
    }
    return NULL;
}

//FETCHER: Ищет работу у других, когда локально пусто 
void* fetcher(void* args) {
    int attempts = 0;

    while (!stop_system) {
        pthread_mutex_lock(&mutex_queue);
        // Ждем, пока очередь не станет пустой или почти пустой
        while (jobQueue.size() > 2 && !stop_system) {
            pthread_cond_wait(&cond_need_work, &mutex_queue);
        }
        if (stop_system) {
            pthread_mutex_unlock(&mutex_queue);
            break;
        }
        pthread_mutex_unlock(&mutex_queue);

        // Начинаем опрашивать соседей
        int target = (rank_id + 1) % comm_size; // Простейшая стратегия: спросить соседа справа
        bool received_job = false;

        // Проходим по кругу, спрашивая всех, кроме себя
        for (int i = 0; i < comm_size - 1; ++i) {
            int request_code = rank_id; // Просто отправляем свой ранк как запрос

            // Отправляем запрос
            MPI_Send(&request_code, 1, MPI_INT, target, TAG_REQUEST_JOB, MPI_COMM_WORLD);

            // Ждем ответ (блокирующе, так как мы Fetcher и можем ждать)
            int response_data[2]; // [0] - simulation time, [1] - job id. Если -1, работы нет.
            MPI_Status status;
            MPI_Recv(response_data, 2, MPI_INT, target, TAG_RESPONSE_JOB, MPI_COMM_WORLD, &status);

            if (response_data[0] != -1) {
                
                pthread_mutex_lock(&mutex_queue);
                jobQueue.push(Job(response_data[1], response_data[0]));
                printf("[%d] Stole job %d from %d\n", rank_id, response_data[1], target);
                pthread_mutex_unlock(&mutex_queue);
                received_job = true;
                break; 
            }

            target = (target + 1) % comm_size;
            if (target == rank_id) target = (target + 1) % comm_size;
        }

        if (!received_job) {
            // Если никто не дал работу, это может значить, что работа кончилась глобально.
            // В реальной системе здесь нужен алгоритм завершения (например, Дейкстры-Шолтена).
            // Для упрощения: если мы прошли круг и ничего не нашли, просто чуть ждем.
            usleep(100000); // 100ms

            // Если очередь пуста и мы не нашли работу у других - считаем, что конец.
            pthread_mutex_lock(&mutex_queue);
            if (jobQueue.empty()) {
                stop_system = true; 
                printf("[%d] No work found globally. Stopping.\n", rank_id);
            }
            pthread_mutex_unlock(&mutex_queue);
        }
    }
    return NULL;
}

// RESPONDER: Отвечает на запросы других 
void* responder(void* args) {
    while (!stop_system) {
        int request_rank;
        MPI_Status status;

        // Используем Iprobe, чтобы проверить наличие сообщений, не блокируясь навечно,
        // чтобы можно было проверить флаг stop_system
        int flag = 0;
        MPI_Iprobe(MPI_ANY_SOURCE, TAG_REQUEST_JOB, MPI_COMM_WORLD, &flag, &status);

        if (flag) {
            MPI_Recv(&request_rank, 1, MPI_INT, status.MPI_SOURCE, TAG_REQUEST_JOB, MPI_COMM_WORLD, &status);

            int response[2] = { -1, -1 }; // По умолчанию: работы нет

            pthread_mutex_lock(&mutex_queue);
            // Если у нас много работы (больше 1), можем поделиться
            if (jobQueue.size() > 1) {
                Job job = jobQueue.front();
                jobQueue.pop(); // Забираем у себя
                response[0] = job.jobSimulationTime;
                response[1] = job.id;
            }
            pthread_mutex_unlock(&mutex_queue);

            MPI_Send(response, 2, MPI_INT, status.MPI_SOURCE, TAG_RESPONSE_JOB, MPI_COMM_WORLD);
        }
        else {
            // Если сообщений нет, немного спим или проверяем условие выхода
            usleep(1000);
            //  в реальном коде Responder должен уметь получать спец-сообщение о завершении.
            // Здесь он завершится по глобальному флагу stop_system, который выставит Fetcher/Main.
        }
    }
    return NULL;
}

int main(int argc, char** argv) {
    int required = MPI_THREAD_MULTIPLE;
    int provided;
    MPI_Init_thread(&argc, &argv, required, &provided);

    if (required != provided) {
        std::cerr << "MPI implementation does not support MPI_THREAD_MULTIPLE" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank_id);

    // Инициализация мьютексов
    pthread_mutex_init(&mutex_queue, NULL);
    pthread_cond_init(&cond_need_work, NULL);

    // ГЕНЕРАЦИЯ ЗАДАЧ

    if (rank_id == 0) {
        for (int i = 0; i < 20; ++i) {
            jobQueue.push(Job(i, 10 + (i * 2))); // id, time (ms)
        }
        printf("[%d] Generated initial jobs.\n", rank_id);
    }

    pthread_t t_worker, t_fetcher, t_responder;

    pthread_create(&t_worker, NULL, worker, NULL);
    pthread_create(&t_fetcher, NULL, fetcher, NULL);
    pthread_create(&t_responder, NULL, responder, NULL);

    pthread_join(&t_worker);
    pthread_join(&t_fetcher);
    // Responder здесь можно принудительно остановить или послать ему сигнал
    // В простом примере ждем join, полагаясь на stop_system
    pthread_join(&t_responder);

    printf("[%d] All threads finished.\n", rank_id);

    pthread_mutex_destroy(&mutex_queue);
    pthread_cond_destroy(&cond_need_work);

    MPI_Finalize();
    return 0;
}