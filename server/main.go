package main

import (
	"log"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
	// amqp - сокращенное имя пакета
)

func main() {

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	// открывает TCP-соединение на localhost:5672
	// amqp:// — схема протокола.
	// guest:guest — логин:пароль.
	// localhost:5672 — хост:порт.
	// / — виртуальный хост (по умолчанию)
	if err != nil {
		log.Fatalf("не удалось подключиться к RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	// Channel - отдельный поток внутри одного TCP-соединения
	// в RabbitMQ все операции идут через каналы
	if err != nil {
		log.Fatalf("не удалось открыть канал: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"rpc_queue", // name
		false,       // durable - переживёт рестарт брокера? (false = нет)
		false,       // autoDelete - удалить, когда нет потребителей? (false = нет)
		false,       // exclusive - только этому соединению? (false = нет)
		false,       // noWait - не ждать ответа сервера? (false = ждать)
		nil,         // arguments - доп. параметры (nil = нет)
	)
	if err != nil {
		log.Fatalf("не удалось объявить очередь: %v", err)
	}

	log.Printf("Очередь %q готова, ждём RPC-запросы...", q.Name)

	msgs, err := ch.Consume(
		q.Name, // имя очереди
		"",     // consumer tag (пустая строка = автогенерация)
		true,   // autoAck — автоматически подтверждать получение?
		false,  // exclusive — только этому потребителю?
		false,  // noLocal — не получать свои же сообщения?
		false,  // noWait — не ждать подтверждения?
		nil,    // arguments
	)
	if err != nil {
		log.Fatalf("не удалось начать чтение: %v", err)
	}

	log.Printf("Слушаем очередь %q...", q.Name)

	for msg := range msgs {
		n, err := strconv.Atoi(string(msg.Body))
		if err != nil {
			log.Printf("не удалось распарсить число: %v", err)
			continue // пропустить это сообщение, продолжить цикл
			// прерывает текущую итерацию и переходит к следующей
		}
		result := n * 2
		log.Printf("Получено N=%d, вычислено N*2=%d", n, result)

		err = ch.Publish(
			"",          // exchange (default) специальное имя для дефолтного обменника
			msg.ReplyTo, // routing key = имя очереди ответа
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				// как в постмане
				ContentType:   "text/plain",
				CorrelationId: msg.CorrelationId,
				// важно, возвращаем тот же ID
				// ID запроса
				Body: []byte(strconv.Itoa(result)),
				// AMQP работает с байтами
			},
		)
		if err != nil {
			log.Printf("не удалось опубликовать ответ: %v", err)
			continue
		}
		log.Printf("Ответ %d отправлен в %q", result, msg.ReplyTo)
		// msg.ReplyTo имя очереди, куда мы послали

	}

}
