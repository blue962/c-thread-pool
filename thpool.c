#include "thpool.h"
#include <pthread.h>
#include <stdlib.h>

// 任务 - 设计
typedef struct job{
    // 与其他任务的连接
    struct job * next;

    // 执行函数
    void(*func)(void *); // 通用地址void *

    // 参数地址
    void *arg;
}job;

// 任务队列 - 设计
typedef struct jobqueue{

    // 互斥锁
    pthread_mutex_t mutex;
    // 条件变量
    pthread_cond_t has_cond;
    // 队头指针
    job *front;
    // 队尾指针
    job *rear;
    // 队长
    int len;
    
}jobqueue;

// 任务队列 - 初始化
int jobqueue_init(jobqueue *p){
    if(pthread_mutex_init(&(p->mutex),NULL) != 0){
        return -1;
    }
    if(pthread_cond_init(&(p->has_cond),NULL) != 0){
        pthread_mutex_destroy(&(p->mutex)); // 初始化条件变量失败 需要销毁已经创建的互斥锁
        return -1;
    }
    p->front = NULL;
    p->rear = NULL;
    p->len = 0;

    return 0;
}

// 任务队列 - 放任务
void jobqueue_push(jobqueue *queue,job *newjob){
    // 任务的next指针置空 还没进入队列，不需要占用队列的mutex
    newjob->next = NULL;
    // 队列加锁
    pthread_mutex_lock(&(queue->mutex));
    // 添加任务到队尾
    if(queue->len == 0){    // 空队列
        queue->front = newjob;
        queue->rear = newjob;

    }
    else{
        queue->rear->next = newjob;
        queue->rear = newjob;
    }
    queue->len++;
    pthread_cond_signal(&(queue->has_cond));   // 唤醒一个阻塞的线程
    // 队列解锁
    pthread_mutex_unlock(&(queue->mutex));
}

// 任务队列 - 取任务
job *jobqueue_pull(jobqueue *queue){
    // 队列加锁
    pthread_mutex_lock(&(queue->mutex));

    // 从队头取任务
    if(queue->len == 0){    // 空队列
        pthread_mutex_unlock(&(queue->mutex));
        return NULL;    // 结束之前要注意有没有解锁 不然这把锁就没人解锁了
    }

    // 临时指针 存放第一个任务的地址
    job *temp_job = queue->front;
    if(queue->front == queue->rear){   // 队列只有一个任务
        queue->front = NULL;
        queue->rear = NULL;
    }
    else{ // 队列任务>1
        queue->front = queue->front->next;
    }
    // 取完任务 任务个数减一
    queue->len--;
    
    // 队列解锁
    pthread_mutex_unlock(&(queue->mutex));

    // 取出的任务 next应该置空 彻底脱离链表队列 并且已经不属于队列,修改不要占用任务队列的mutex 减少占用锁的时间
    // 临界区尽量短 把需要保护的共享资源操作放到锁里面
    temp_job->next = NULL;

    return temp_job;
}

// 工作线程 - 设计
typedef struct thread{
    int id; // 自定义的工作线程编号 方便管理调试
    pthread_t thread_id;    // 线程标识
    struct thpool *thpool_p;   // 指向所属的线程池
}thread;

// 线程池 - 设计
typedef struct thpool{
    thread **threads;   // 指向线程的指针 管理所有线程
    int threads_num;    // 线程个数
    jobqueue jobqueue;  // 线程池共用的任务队列

}thpool;

// 工作线程入口
void *thread_do(void *arg){
    // pthread_create 传入的是 thread *，
    // 这里将通用指针 void * 转回 thread *
    thread *thread_p = (thread *)arg;   // 新定义一个指针变量，让它指向 当前这个工作线程对应的 thread 结构体

    // 通过当前工作线程找到所属线程池
    thpool *pool = thread_p ->thpool_p;

    // 获取线程池中的公共任务队列
    jobqueue *queue = &pool->jobqueue;

    while (1)   // 线程要不断地查看是否有任务，所以是死循环
    {
        pthread_mutex_lock(&(queue->mutex));
        // while循环判断 因为醒来还要进行判断是否有任务
        while (queue->len == 0) {
            pthread_cond_wait(&(queue->has_cond),&(queue->mutex));  // 会进行睡眠 等待信号唤醒  睡之前要释放锁 睡醒会重新拿锁
        }

        pthread_mutex_unlock(&(queue->mutex));

        job *job_p = jobqueue_pull(queue);
        if(job_p != NULL){
            job_p->func(job_p->arg);
            free(job_p);    // 任务完成要进行释放 生命周期结束
        }
    }
    

    return NULL;
}

/** 初始化工作线程
 *  malloc一个线程
 *  设置id
 *  设置所属线程池指针 thpool_p
 *  pthread_create 创建真正的工作线程
 * 
 *  @param thpool_p 要绑定的线程池
 *  @param thread_p 返回创建的线程地址
 *  @param id 线程id
 */
int thread_init(thpool *pool,thread **thread_p,int id){

    // 为工作线程的“线程信息结构体”申请内存
    *thread_p = malloc(sizeof(thread));
    // malloc 失败则无法创建工作线程
    if(*thread_p == NULL){
        return -1;
    }

    // 保存自定义线程编号，方便后续管理和调试
    (*thread_p)->id = id;

    // 保存所属线程池地址
    // 工作线程以后可以通过它找到公共任务队列等资源
    (*thread_p)->thpool_p = pool;

    // 创建真正的 POSIX 工作线程
    int ret = pthread_create(
        &(*thread_p)->thread_id,    // 保存新线程的 pthread 标识
        NULL,   // 使用默认线程属性
        thread_do,  // 新线程启动后执行的入口函数
        *thread_p   // 传给 thread_do() 的参数
    );

    // pthread_create 创建失败
    if(ret != 0){
        // 释放前面 malloc 的 thread 结构体，避免内存泄漏
        free(*thread_p);
        // 防止外部留下悬空指针
        *thread_p = NULL;
        return -1;
    }

    return 0;
}

// 初始化线程池
thpool *thpool_init(int threads_num){
    thpool *pool = malloc(sizeof(thpool));
    if(pool == NULL){
        return NULL;
    }
    pool->threads_num = threads_num;

    if (jobqueue_init(&(pool->jobqueue)) != 0) {
        free(pool);
        return NULL;
    }
    pool->threads = malloc(sizeof(thread *) * threads_num);
    
    for (int i = 0; i < threads_num; i++){
        thread_init(pool,&(pool->threads[i]),i);
    }
}
