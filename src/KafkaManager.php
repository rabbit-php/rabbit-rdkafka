<?php
declare(strict_types=1);

namespace Rabbit\Rdkafka;

use Psr\Log\LoggerInterface;
use rabbit\contract\InitInterface;
use rabbit\exception\InvalidArgumentException;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use RdKafka\Conf;
use RdKafka\Consumer;
use RdKafka\ConsumerTopic;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;
use RdKafka\ProducerTopic;
use RdKafka\Topic;
use RdKafka\TopicConf;

/**
 * Class Manager
 * @package Rabbit\Rdkafka
 */
class KafkaManager implements InitInterface
{
    /** @var array */
    protected $producers = [];
    /** @var array */
    protected $consumers = [];
    /** @var array */
    protected $kafkaConsumers = [];
    /** @var array */
    const LOG_LEVEL = [
        0 => 'emergency',
        1 => 'alert',
        2 => 'critical',
        3 => 'error',
        4 => 'warning',
        5 => 'notice',
        6 => 'info',
        7 => 'debug',
        8 => 'debug'
    ];

    const ITEM_ENGINE = 'engine';
    const ITEM_TOPICS = 'topics';
    /** @var array */
    protected $configs = [];

    /**
     * Manager constructor.
     */
    public function __construct(array $configs = [])
    {
        $this->configs = $configs;
        register_shutdown_function(function () {
            foreach ($this->producers as $producer) {
                $producer[self::ITEM_ENGINE]->flush(10000);
            }
        });
    }

    public function init()
    {
        $this->makeKafka($this->configs);
    }


    /**
     * @param string $name
     * @return Producer|null
     */
    public function getProducer(string $name): ?Producer
    {
        return ArrayHelper::getValue($this->producers, $name . '.' . self::ITEM_ENGINE);
    }

    /**
     * @param string $name
     * @return Consumer|null
     */
    public function getConsumer(string $name): ?Consumer
    {
        return ArrayHelper::getValue($this->consumers, $name . '.' . self::ITEM_ENGINE);
    }

    /**
     * @param string $name
     * @return KafkaConsumer|null
     */
    public function getKafkaConsumer(string $name): ?KafkaConsumer
    {
        return ArrayHelper::getValue($this->kafkaConsumers, $name . '.' . self::ITEM_ENGINE);
    }

    /**
     * @param string $name
     * @param Producer $producer
     */
    public function setProducer(string $name, Producer $producer): void
    {
        if (isset($this->producers[$name])) {
            throw new InvalidArgumentException("The $name already exists");
        }
        $this->producers[$name] = $producer;
    }

    /**
     * @param string $name
     * @param Consumer $consumer
     */
    public function setConsumer(string $name, Consumer $consumer): void
    {
        if (isset($this->consumers[$name])) {
            throw new InvalidArgumentException("The $name already exists");
        }
        $this->consumers[$name] = $producer;
    }

    /**
     * @param string $name
     * @param Consumer $consumer
     */
    public function setKafkaConsumer(string $name, KafkaConsumer $consumer): void
    {
        if (isset($this->kafkaConsumers[$name])) {
            throw new InvalidArgumentException("The $name already exists");
        }
        $this->kafkaConsumers[$name] = $producer;
    }

    /**
     * @param string $name
     * @param string $topic
     * @param array $set
     * @return ProducerTopic|null
     */
    public function getProducerTopic(string $name, string $topic, array $set = []): ?ProducerTopic
    {
        return $this->getTopic('producers', $name, $topic, $set);
    }

    /**
     * @param string $name
     * @param string $topic
     * @param array $set
     * @return ConsumerTopic|null
     */
    public function getConsumerTopic(string $name, string $topic, array $set = []): ?ConsumerTopic
    {
        return $this->getTopic('consumers', $name, $topic, $set);
    }

    /**
     * @param string $name
     * @param string $topic
     * @param array $set
     * @return ConsumerTopic|null
     */
    public function getKafkaConsumerTopic(string $name, string $topic, array $set = []): ?ConsumerTopic
    {
        return $this->getTopic('kafkaConsumers', $name, $topic, $set);
    }

    /**
     * @param array $config
     * @throws InvalidConfigException
     */
    public function makeKafka(array $configs): void
    {
        foreach ($configs as $name => $config) {
            [
                $dsn,
                $set,
                $logger,
                $type
            ] = ArrayHelper::getValueByArray($config, [
                'dsn',
                'set',
                'logger',
                'type',
            ], null, [
                'set' => []
            ]);
            if ($dsn === null || $type === null) {
                throw new InvalidConfigException("dsn and type can not be empty!");
            }
            $name = (string)$name;
            $conf = new Conf();
            $conf->set('bootstrap.servers', is_array($dsn) ? implode(',', $dsn) : $dsn);
            foreach ($set as $key => $value) {
                $conf->set((string)$key, (string)$value);
            }
            if ($logger instanceof LoggerInterface) {
                $conf->setLogCb(function ($kafka, $level, $facility, $message) use ($logger) {
                    $logger->{self::LOG_LEVEL[$level]}("$facility $message", ['module' => 'kafka']);
                });
            }
            switch ($type) {
                case 'producer':
                    $this->producers[$name][self::ITEM_ENGINE] = new Producer($conf);
                    break;
                case 'consumer':
                    $this->consumers[$name][self::ITEM_ENGINE] = new Consumer($conf);
                    break;
                case 'kafkaconsumer':
                    $conf->setRebalanceCb(function (KafkaConsumer $kafka, $err, array $partitions = null) {
                        switch ($err) {
                            case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                                $kafka->assign($partitions);
                                break;

                            case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                                $kafka->assign(NULL);
                                break;
                            default:
                                throw new \Exception($err);
                        }
                    });
                    $this->kafkaConsumers[$name][self::ITEM_ENGINE] = new KafkaConsumer($conf);
                    break;
                default:
                    throw new InvalidConfigException("Not support type $type");
            }
        }
    }

    /**
     * @param string $type
     * @param string $name
     * @param string $topic
     * @param array $set
     * @return Topic|null
     */
    protected function getTopic(string $type, string $name, string $topic, array $set): ?Topic
    {
        if (!isset($this->$type[$name])) {
            return null;
        }
        if (null === $topicModel = ArrayHelper::getValue($this->$type, 'topics' . $topic)) {
            $conf = new TopicConf();
            foreach ($set as $key => $value) {
                $conf->set((string)$key, (string)$value);
            }
            $topicModel = $this->$type[$name][self::ITEM_ENGINE]->newTopic($topic, $conf);
            $this->$type[$name][self::ITEM_TOPICS][$topic] = $topicModel;
        }
        return $topicModel;
    }
}