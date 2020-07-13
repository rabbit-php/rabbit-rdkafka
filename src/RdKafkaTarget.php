<?php
declare(strict_types=1);

namespace Rabbit\Rdkafka;

use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\StringHelper;
use Rabbit\Log\Targets\AbstractTarget;
use Rabbit\Rdkafka\KafkaManager;

/**
 * Class RdKafkaTarget
 * @package Rabbit\Rdkafka
 */
class RdKafkaTarget extends AbstractTarget
{
    /** @var array */
    private array $template = [
        ['datetime', 'timespan'],
        ['level', 'string'],
        ['request_uri', 'string'],
        ['request_method', 'string'],
        ['clientip', 'string'],
        ['requestid', 'string'],
        ['filename', 'string'],
        ['memoryusage', 'int'],
        ['message', 'string']
    ];
    /** @var string */
    private string $producer;
    /** @var string */
    private string $key = 'kafka';
    private string $topic = 'seaslog';
    /** @var int */
    private int $ack = 0;
    /** @var float */
    private float $autoCommit = 1 * 1000;

    /**
     * KafkaTarget constructor.
     * @param string $producer
     * @param string $key
     */
    public function __construct(string $producer, string $key = 'kafka')
    {
        parent::__construct();
        $this->producer = $producer;
        $this->key = $key;
    }


    /**
     * @param array $messages
     */
    public function export(array $messages): void
    {
        foreach ($messages as $module => $message) {
            foreach ($message as $msg) {
                if (is_string($msg)) {
                    switch (ini_get('seaslog.appender')) {
                        case '2':
                        case '3':
                            $msg = trim(substr($msg, StringHelper::str_n_pos($msg, ' ', 6)));
                            break;
                        case '1':
                        default:
                            $fileName = basename($module);
                            $module = substr($fileName, 0, strrpos($fileName, '_'));
                    }
                    $msg = explode($this->split, trim($msg));
                } else {
                    ArrayHelper::remove($msg, '%c');
                }
                if (!empty($this->levelList) && !in_array($msg[$this->levelIndex], $this->levelList)) {
                    continue;
                }
                $log = [
                    'appname' => $module,
                ];
                foreach ($msg as $index => $value) {
                    [$name, $type] = $this->template[$index];
                    switch ($type) {
                        case "string":
                            $log[$name] = trim($value);
                            break;
                        case "int":
                            $log[$name] = (int)$value;
                            break;
                        default:
                            $log[$name] = trim($value);
                    }
                }
                $this->channel->push(json_encode($log));
            }
        }
    }

    public function write(): void
    {
        loop(function () {
            $logs = $this->getLogs();
            if (!empty($logs)) {
                /** @var KafkaManager $kafka */
                $kafka = getDI($this->key);
                $kafka->product($kafka->getProducerTopic($this->producer, $this->topic, [
                    'acks' => $this->ack,
                    'auto.commit.interval.ms' => $this->autoCommit
                ]), $kafka->getProducer($this->producer), RD_KAFKA_PARTITION_UA, 0, implode(',', $logs));
            }
        });
    }
}