using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
namespace Sender
{
    internal class SendHello
    {
        public static async Task SendOnce()
        {
            string queue_name = "GreetingQueue";
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
            await channel.QueueDeclareAsync(queue: queue_name, durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } });
            //todo 发送消息
            string message = "";
            message = "Helle World!";
            var body = Encoding.UTF8.GetBytes(message);
            await channel.BasicPublishAsync(exchange: "", routingKey: queue_name, body: body);
            Console.WriteLine($" [x] Sent {message}");
            //todo 退出消息
            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();

        }

        public static async Task SendContinuesly()
        {
            string queue_name = "GreetingQueue";
            //实例化连接工厂：指定主机名，端口好，用户名和密码（如果不指定用户和密码，会默认使用guest登录）
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                Port = 5672,
                UserName = "ls",
                Password = "ls"
            };
            //创建连接（使用using语句，确保连接在使用后被正确关闭）
            await using var connection = await factory.CreateConnectionAsync();
            //通过创建此参数对象来启用发布确认和发布确认跟踪功能
            var channelOptions = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
            //创建通道
            await using var channel = await connection.CreateChannelAsync(channelOptions);
            //todo 声明队列
            await channel.QueueDeclareAsync(queue: queue_name, durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } });
            //todo 发送消息
            while (true)
            {
                Console.Write("请输入要发送的消息(bye退出)：");
                string? msg = Console.ReadLine()?.Trim();
                if (string.IsNullOrEmpty(msg) || string.IsNullOrWhiteSpace(msg))
                {
                    Console.WriteLine("输入为空，请重新输入！ ");
                    continue;
                }
                var body = Encoding.UTF8.GetBytes(msg);
                await channel.BasicPublishAsync(exchange: "", routingKey: queue_name, body: body);
                Console.WriteLine($" [x] Sent >>{msg}");
                if(string.Equals(msg, "bye", StringComparison.OrdinalIgnoreCase))
                {
                    break; //收到bye消息，退出循环
                }
            }
        }

        public static async Task Main(string[] args)
        {
            await SendContinuesly();
        }
    }
}
