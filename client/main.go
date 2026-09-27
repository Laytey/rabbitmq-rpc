package main

import (
	"log"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	//подключение
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("не удалось подключиться: %v", err)
	}
	defer conn.Close()

	//канал
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("не удалось открыть канал: %v", err)
	}
	defer ch.Close()

	// временная очередь для ответов
	replyQ, err := ch.QueueDeclare(
		"", // пустое имя - RabbitMQ сгенерирует
		false,
		true, // autoDelete
		true, // exclusive
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("не удалось создать очередь ответов: %v", err)
	}
	log.Printf("Очередь ответов: %q", replyQ.Name)

	// объявить rpc_queue (сервер её создаст сам, но для надёжности)
	_, err = ch.QueueDeclare("rpc_queue", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("не удалось объявить rpc_queue: %v", err)
	}

	// подписаться на очередь ответов
	msgs, err := ch.Consume(replyQ.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("не удалось подписаться на ответы: %v", err)
	}

	// сгенерировать correlation_id
	corrId := strconv.FormatInt(time.Now().UnixNano(), 10)

	// отправить запрос
	n := 10
	err = ch.Publish(
		"",
		"rpc_queue",
		false,
		false,
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       replyQ.Name,
			Body:          []byte(strconv.Itoa(n)),
		},
	)
	if err != nil {
		log.Fatalf("не удалось опубликовать запрос: %v", err)
	}
	log.Printf("Отправлено N=%d, corrId=%s, ждём ответ...", n, corrId)

	// ждать ответ
	for msg := range msgs {
		if msg.CorrelationId == corrId {
			log.Printf("Получен ответ: %s", msg.Body)
			return
		}
		log.Printf("Пропускаем чужой ответ: corrId=%s", msg.CorrelationId)
	}
}
