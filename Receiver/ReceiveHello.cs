using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
namespace Receiver
{
    internal class ReceiveHello
    {
        private const string DEFAULT_QUEUE_NAME = "GreetingQueue";
        public static async Task ReceiveOnce()
        {
            //todo 实例化连接工厂：指定主机名，端口好，用户名和密码（如果不指定用户和密码，会默认使用guest登录）
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                Port = 5672,
                UserName = "ls",
                Password = "ls"
            };
            //todo 创建连接（使用using语句，确保连接在使用后被正确关闭）
            using var connection = await factory.CreateConnectionAsync();
            //todo 创建通道
            using var channel = await connection.CreateChannelAsync();

            //todo 声明队列
            await channel.QueueDeclareAsync(queue: DEFAULT_QUEUE_NAME, durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } });
            //todo 接收消息
            Console.WriteLine(" [*] Waiting for messages.");
            var consumer = new AsyncEventingBasicConsumer(channel);
            //绑定消息订阅事件
            consumer.ReceivedAsync += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($" [x] Received {message}");
                return Task.CompletedTask;//todo 了解这个方法
            };
            await channel.BasicConsumeAsync(queue: DEFAULT_QUEUE_NAME, autoAck: true, consumer: consumer);
            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
        /// <summary>
        /// 该方法使用了TaskCompletionSource来表示任务的完成状态，当收到bye消息时，设置任务完成状态，从而退出等待。
        /// 该方法通过异步实现了持续接受和处理消息，避免了轮询查询的方式，提高了效率。
        /// </summary>
        /// <returns></returns>
        public static async Task ReceiveContinueslyByFinishTag()
        {
            //声明连接工厂
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                Port = 5672,
                UserName = "ls",
                Password = "ls"
            };
            //创建连接
            using var connection = await factory.CreateConnectionAsync();
            //创建通道
            using var channel = await connection.CreateChannelAsync();
            //声明队列
            await channel.QueueDeclareAsync(queue: DEFAULT_QUEUE_NAME, durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } });
            // 关键：限制未确认消息数。quorum 队列不支持 global: true，必须传 false
            // prefetchSize : 0 表示不限制消息体大小，prefetchCount: 2 表示每次只接收一条未确认的消息，global: false 表示只对当前消费者生效。(对于qurom队列，global参数必须为false，否则会报错)
            //如果不设置qos，broker会将队列消息全部推送过来，然后依次执行receivedAsync
            await channel.BasicQosAsync(
                prefetchSize: 0,
                prefetchCount: 2,
                global: false);
            // 定义一个 TaskCompletionSource 来表示任务的完成状态
            var finished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            //注册消费者
            var consumer = new AsyncEventingBasicConsumer(channel);
            //绑定消息订阅事件
            consumer.ReceivedAsync += async (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($" [x] Received >>{message}:)");
                //手动确认消息
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                //如果收到bye消息，设置任务完成状态
                if (string.Equals(message, "bye", StringComparison.OrdinalIgnoreCase))
                {
                    finished.SetResult();
                }

            };
            //开始消费消息
            //BasicConsumeAsync方法返回一个消费者标签（customerTag），用于标识该消费者，可以用于取消消费。
            //该方法返回时只代表消费者注册成功，并不代表已经接收到消息，如果进程结束，那么接收消息的回调也不会执行，因此需要定义一个结束标志来等待消息的接收完成。
            string customerTag = await channel.BasicConsumeAsync(queue: DEFAULT_QUEUE_NAME, autoAck: false, consumer: consumer);
            Console.WriteLine("等待消息中，收到 Bye 后退出……");
            await finished.Task; //等待任务完成

            await channel.BasicCancelAsync(customerTag); //取消消费者,可选的收尾动作，释放channel/connection后也会停止消费，但是显示取消语义更加清晰。
            Console.WriteLine("收到 Bye，程序退出。");
        }
        /// <summary>
        /// 该方法使用了轮询查询的方式来持续接收消息，通过BasicGetAsync方法获取队列中的消息，如果没有消息则等待一段时间后继续查询。
        /// 注意：该方法在高并发场景下可能会导致性能问题，因为轮询查询会频繁访问队列，增加了RabbitMQ的负载。
        /// </summary>
        /// <returns></returns>
        public static async Task ReceiveContinueslyByLoopQuery()
        {
            //声明连接工厂
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                Port = 5672,
                UserName = "ls",
                Password = "ls"
            };
            //创建连接
            await using var connection = await factory.CreateConnectionAsync();
            //创建通道
            await using var channel = await connection.CreateChannelAsync();
            //声明队列：必须await，保证队列在第一次BasicGetAsync之前已经声明完成，否则会报错：NOT_FOUND - no queue 'GreetingQueue' in vhost '/'
            await channel.QueueDeclareAsync(queue: DEFAULT_QUEUE_NAME, durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } });

            while (true)
            {
                var result = await channel.BasicGetAsync(queue: DEFAULT_QUEUE_NAME, autoAck: false);//设置了等待，因此每次循环都是一次同步往返处理，即使队列为空也在访问broker，且不受prefetch限制。

                if (result is null)
                {
                    await Task.Delay(1000); //等待一段时间后继续查询
                    continue;
                }
                var body = result.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($" [x] Received {message}");
                await channel.BasicAckAsync(result.DeliveryTag, multiple: false);

                if (string.Equals(message?.Trim(), "bye", StringComparison.OrdinalIgnoreCase))
                {
                    break; //收到bye消息，退出循环
                }
            }
            Console.WriteLine("收到 Bye，程序退出。");
        }
        public static async Task Main(string[] args)
        {
            await ReceiveContinueslyByFinishTag();
            //await ReceiveContinueslyByLoopQuery();
        }
    }
}
