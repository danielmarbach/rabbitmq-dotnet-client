﻿#pragma warning disable CA2007 // Consider calling ConfigureAwait on the awaited task
using System.Text;
using RabbitMQ.Client;

ConnectionFactory connectionFactory = new()
{
    AutomaticRecoveryEnabled = true,
    UserName = "guest",
    Password = "guest"
};

var props = new BasicProperties();
byte[] msg = Encoding.UTF8.GetBytes("test");
await using var connection = await connectionFactory.CreateConnectionAsync();
for (int i = 0; i < 300; i++)
{
    try
    {
        await using var channel = await connection.CreateChannelAsync(); // New channel for each message
        await Task.Delay(1000);
        await channel.BasicPublishAsync(exchange: string.Empty, routingKey: string.Empty,
            mandatory: false, basicProperties: props, body: msg);
        Console.WriteLine($"Sent message {i}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Failed to send message {i}: {ex.Message}");
        await Task.Delay(1000);
    }
}
