<?php

namespace Elasticsearch;

use Elasticsearch\Common\Exceptions\BadMethodCallException;
use Elasticsearch\Common\Exceptions\InvalidArgumentException;
use Elasticsearch\Common\Exceptions\NoNodesAvailableException;
use Elasticsearch\Common\Exceptions\BadRequest400Exception;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use Elasticsearch\Common\Exceptions\TransportException;
use Elasticsearch\Endpoints\AbstractEndpoint;
use Elasticsearch\Namespaces\AbstractNamespace;
use Elasticsearch\Namespaces\CatNamespace;
use Elasticsearch\Namespaces\ClusterNamespace;
use Elasticsearch\Namespaces\IndicesNamespace;
use Elasticsearch\Namespaces\IngestNamespace;
use Elasticsearch\Namespaces\NamespaceBuilderInterface;
use Elasticsearch\Namespaces\NodesNamespace;
use Elasticsearch\Namespaces\RemoteNamespace;
use Elasticsearch\Namespaces\SnapshotNamespace;
use Elasticsearch\Namespaces\BooleanRequestWrapper;
use Elasticsearch\Namespaces\TasksNamespace;

/**
 * Class Client
 *
 * @category Elasticsearch
 * @package  Elasticsearch
 * @author   Zachary Tong <zach@elastic.co>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache2
 * @link     http://elastic.co
 */
class Client implements ClientInterface
{
    /**
     * @var Transport
     */
    public $transport;

    /**
     * @var array
     */
    protected $params;

    /**
     * @var IndicesNamespace
     */
    protected $indices;

    /**
     * @var ClusterNamespace
     */
    protected $cluster;

    /**
     * @var NodesNamespace
     */
    protected $nodes;

    /**
     * @var SnapshotNamespace
     */
    protected $snapshot;

    /**
     * @var CatNamespace
     */
    protected $cat;

    /**
     * @var IngestNamespace
     */
    protected $ingest;

    /**
     * @var TasksNamespace
     */
    protected $tasks;

    /**
     * @var RemoteNamespace
     */
    protected $remote;

    /**
     * @var callable
     */
    protected $endpoints;

    /**
     * @var  NamespaceBuilderInterface[]
     */
    protected $registeredNamespaces = [];

    /**
     * Client constructor
     *
     * @param Transport $transport
     * @param callable $endpoint
     * @param AbstractNamespace[] $registeredNamespaces
     */
    public function __construct(Transport $transport, callable $endpoint, array $registeredNamespaces)
    {
        $this->transport = $transport;
        $this->endpoints = $endpoint;
        $this->indices   = new IndicesNamespace($transport, $endpoint);
        $this->cluster   = new ClusterNamespace($transport, $endpoint);
        $this->nodes     = new NodesNamespace($transport, $endpoint);
        $this->snapshot  = new SnapshotNamespace($transport, $endpoint);
        $this->cat       = new CatNamespace($transport, $endpoint);
        $this->ingest    = new IngestNamespace($transport, $endpoint);
        $this->tasks     = new TasksNamespace($transport, $endpoint);
        $this->remote    = new RemoteNamespace($transport, $endpoint);
        $this->registeredNamespaces = $registeredNamespaces;
    }

    /**
     * {@inheritdoc}
     */
    public function info($params = [])
    {
        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Info $endpoint */
        $endpoint = $endpointBuilder('Info');
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function ping($params = [])
    {
        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Ping $endpoint */
        $endpoint = $endpointBuilder('Ping');
        $endpoint->setParams($params);

        try {
            $this->performRequest($endpoint);
        } catch (Missing404Exception $exception) {
            return false;
        } catch (TransportException $exception) {
            return false;
        } catch (NoNodesAvailableException $exception) {
            return false;
        }

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function get($params)
    {
        $id = $this->extractArgument($params, 'id');
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Get $endpoint */
        $endpoint = $endpointBuilder('Get');
        $endpoint->setID($id)
                 ->setIndex($index)
                 ->setType($type);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function getSource($params)
    {
        $id = $this->extractArgument($params, 'id');
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Get $endpoint */
        $endpoint = $endpointBuilder('Get');
        $endpoint->setID($id)
                 ->setIndex($index)
                 ->setType($type)
                 ->returnOnlySource();
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function delete($params)
    {
        $id = $this->extractArgument($params, 'id');
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');

        $this->verifyNotNullOrEmpty("id", $id);
        $this->verifyNotNullOrEmpty("type", $type);
        $this->verifyNotNullOrEmpty("index", $index);

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Delete $endpoint */
        $endpoint = $endpointBuilder('Delete');
        $endpoint->setID($id)
                 ->setIndex($index)
                 ->setType($type);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteByQuery($params = array())
    {
        $index = $this->extractArgument($params, 'index');

        $type = $this->extractArgument($params, 'type');

        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\DeleteByQuery $endpoint */
        $endpoint = $endpointBuilder('DeleteByQuery');
        $endpoint->setIndex($index)
                ->setType($type)
                ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function count($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Count $endpoint */
        $endpoint = $endpointBuilder('Count');
        $endpoint->setIndex($index)
                 ->setType($type)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function countPercolate($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $type  = $this->extractArgument($params, 'type');
        $id    = $this->extractArgument($params, 'id');
        $body  = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\CountPercolate $endpoint */
        $endpoint = $endpointBuilder('CountPercolate');
        $endpoint->setIndex($index)
                 ->setType($type)
                 ->setID($id)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function percolate($params)
    {
        $index = $this->extractArgument($params, 'index');
        $type  = $this->extractArgument($params, 'type');
        $id    = $this->extractArgument($params, 'id');
        $body  = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Percolate $endpoint */
        $endpoint = $endpointBuilder('Percolate');
        $endpoint->setIndex($index)
                 ->setType($type)
                 ->setID($id)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function mpercolate($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\MPercolate $endpoint */
        $endpoint = $endpointBuilder('MPercolate');
        $endpoint->setIndex($index)
                 ->setType($type)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function termvectors($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $type  = $this->extractArgument($params, 'type');
        $id    = $this->extractArgument($params, 'id');
        $body  = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\TermVectors $endpoint */
        $endpoint = $endpointBuilder('TermVectors');
        $endpoint->setIndex($index)
                 ->setType($type)
                 ->setID($id)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function mtermvectors($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $type  = $this->extractArgument($params, 'type');
        $body  = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\MTermVectors $endpoint */
        $endpoint = $endpointBuilder('MTermVectors');
        $endpoint->setIndex($index)
                 ->setType($type)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function exists($params)
    {
        $id = $this->extractArgument($params, 'id');
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');

        //manually make this verbose so we can check status code
        $params['client']['verbose'] = true;

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Exists $endpoint */
        $endpoint = $endpointBuilder('Exists');
        $endpoint->setID($id)
                 ->setIndex($index)
                 ->setType($type);
        $endpoint->setParams($params);

        return BooleanRequestWrapper::performRequest($endpoint, $this->transport);
    }

    /**
     * {@inheritdoc}
     */
    public function mget($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Mget $endpoint */
        $endpoint = $endpointBuilder('Mget');
        $endpoint->setIndex($index)
                 ->setType($type)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function msearch($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Msearch $endpoint */
        $endpoint = $endpointBuilder('Msearch');
        $endpoint->setIndex($index)
                 ->setType($type)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function msearchTemplate($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\MsearchTemplate $endpoint */
        $endpoint = $endpointBuilder('MsearchTemplate');
        $endpoint->setIndex($index)
            ->setType($type)
            ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function create($params)
    {
        $id = $this->extractArgument($params, 'id');
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Create $endpoint */
        $endpoint = $endpointBuilder('Create');
        $endpoint->setID($id)
                 ->setIndex($index)
                 ->setType($type)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function bulk($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Bulk $endpoint */
        $endpoint = $endpointBuilder('Bulk');
        $endpoint->setIndex($index)
                 ->setType($type)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function index($params)
    {
        $id = $this->extractArgument($params, 'id');
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Index $endpoint */
        $endpoint = $endpointBuilder('Index');
        $endpoint->setID($id)
                 ->setIndex($index)
                 ->setType($type)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function reindex($params)
    {
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;
        /** @var \Elasticsearch\Endpoints\Reindex $endpoint */
        $endpoint = $endpointBuilder('Reindex');
        $endpoint->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function suggest($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Suggest $endpoint */
        $endpoint = $endpointBuilder('Suggest');
        $endpoint->setIndex($index)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function explain($params)
    {
        $id = $this->extractArgument($params, 'id');
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Explain $endpoint */
        $endpoint = $endpointBuilder('Explain');
        $endpoint->setID($id)
                 ->setIndex($index)
                 ->setType($type)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function search($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Search $endpoint */
        $endpoint = $endpointBuilder('Search');
        $endpoint->setIndex($index)
                 ->setType($type)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function searchShards($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\SearchShards $endpoint */
        $endpoint = $endpointBuilder('SearchShards');
        $endpoint->setIndex($index)
                 ->setType($type);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function searchTemplate($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Search $endpoint */
        $endpoint = $endpointBuilder('SearchTemplate');
        $endpoint->setIndex($index)
                 ->setType($type)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function scroll($params = array())
    {
        $scrollID = $this->extractArgument($params, 'scroll_id');
        $body = $this->extractArgument($params, 'body');
        $scroll = $this->extractArgument($params, 'scroll');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Scroll $endpoint */
        $endpoint = $endpointBuilder('Scroll');
        $endpoint->setScrollID($scrollID)
                 ->setScroll($scroll)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function clearScroll($params = array())
    {
        $scrollID = $this->extractArgument($params, 'scroll_id');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\ClearScroll $endpoint */
        $endpoint = $endpointBuilder('ClearScroll');
        $endpoint->setScrollID($scrollID)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     * $params['id']                = (string) Document ID (Required)
     *        ['index']             = (string) The name of the index (Required)
     *        ['type']              = (string) The type of the document (Required)
     *        ['consistency']       = (enum) Explicit write consistency setting for the operation
     *        ['fields']            = (list) A comma-separated list of fields to return in the response
     *        ['lang']              = (string) The script language (default: mvel)
     *        ['parent']            = (string) ID of the parent document
     *        ['refresh']           = (boolean) Refresh the index after performing the operation
     *        ['replication']       = (enum) Specific replication type
     *        ['retry_on_conflict'] = (number) Specify how many times should the operation be retried when a conflict occurs (default: 0)
     *        ['routing']           = (string) Specific routing value
     *        ['script']            = () The URL-encoded script definition (instead of using request body)
     *        ['timeout']           = (time) Explicit operation timeout
     *        ['timestamp']         = (time) Explicit timestamp for the document
     *        ['ttl']               = (duration) Expiration time for the document
     *        ['version_type']      = (number) Explicit version number for concurrency control
     *        ['body']              = (array) The request definition using either `script` or partial `doc`
     *
     * @param $params array Associative array of parameters
     *
     * @return array
     */
    public function update($params)
    {
        $id = $this->extractArgument($params, 'id');
        $index = $this->extractArgument($params, 'index');
        $type = $this->extractArgument($params, 'type');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Update $endpoint */
        $endpoint = $endpointBuilder('Update');
        $endpoint->setID($id)
                 ->setIndex($index)
                 ->setType($type)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function updateByQuery($params = array())
    {
        $index = $this->extractArgument($params, 'index');

        $body = $this->extractArgument($params, 'body');

        $type = $this->extractArgument($params, 'type');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\UpdateByQuery $endpoint */
        $endpoint = $endpointBuilder('UpdateByQuery');
        $endpoint->setIndex($index)
            ->setType($type)
            ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function getScript($params)
    {
        $id = $this->extractArgument($params, 'id');
        $lang = $this->extractArgument($params, 'lang');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Script\Get $endpoint */
        $endpoint = $endpointBuilder('Script\Get');
        $endpoint->setID($id)
                 ->setLang($lang);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteScript($params)
    {
        $id = $this->extractArgument($params, 'id');
        $lang = $this->extractArgument($params, 'lang');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Script\Delete $endpoint */
        $endpoint = $endpointBuilder('Script\Delete');
        $endpoint->setID($id)
                 ->setLang($lang);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function putScript($params)
    {
        $id   = $this->extractArgument($params, 'id');
        $lang = $this->extractArgument($params, 'lang');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Script\Put $endpoint */
        $endpoint = $endpointBuilder('Script\Put');
        $endpoint->setID($id)
                 ->setLang($lang)
                 ->setBody($body);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function getTemplate($params)
    {
        $id = $this->extractArgument($params, 'id');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Template\Get $endpoint */
        $endpoint = $endpointBuilder('Template\Get');
        $endpoint->setID($id);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteTemplate($params)
    {
        $id = $this->extractArgument($params, 'id');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Template\Delete $endpoint */
        $endpoint = $endpointBuilder('Template\Delete');
        $endpoint->setID($id);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function putTemplate($params)
    {
        $id   = $this->extractArgument($params, 'id');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Template\Put $endpoint */
        $endpoint = $endpointBuilder('Template\Put');
        $endpoint->setID($id)
            ->setBody($body)
            ->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function fieldStats($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\FieldStats $endpoint */
        $endpoint = $endpointBuilder('FieldStats');
        $endpoint->setIndex($index)
            ->setBody($body)
            ->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function fieldCaps($params = array())
    {
        $index = $this->extractArgument($params, 'index');
        $body = $this->extractArgument($params, 'body');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\FieldCaps $endpoint */
        $endpoint = $endpointBuilder('FieldCaps');
        $endpoint->setIndex($index)
            ->setBody($body)
            ->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function renderSearchTemplate($params = array())
    {
        $body = $this->extractArgument($params, 'body');
        $id   = $this->extractArgument($params, 'id');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\RenderSearchTemplate $endpoint */
        $endpoint = $endpointBuilder('RenderSearchTemplate');
        $endpoint->setBody($body)
            ->setID($id);
        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * {@inheritdoc}
     */
    public function indices()
    {
        return $this->indices;
    }

    /**
     * {@inheritdoc}
     */
    public function cluster()
    {
        return $this->cluster;
    }

    /**
     * {@inheritdoc}
     */
    public function nodes()
    {
        return $this->nodes;
    }

    /**
     * {@inheritdoc}
     */
    public function snapshot()
    {
        return $this->snapshot;
    }

    /**
     * {@inheritdoc}
     */
    public function cat()
    {
        return $this->cat;
    }

    /**
     * {@inheritdoc}
     */
    public function ingest()
    {
        return $this->ingest;
    }

    /**
     * {@inheritdoc}
     */
    public function tasks()
    {
        return $this->tasks;
    }

    /**
     * {@inheritdoc}
     */
    public function remote()
    {
        return $this->remote;
    }

    /**
     * {@inheritdoc}
     */
    public function __call($name, $arguments)
    {
        if (isset($this->registeredNamespaces[$name])) {
            return $this->registeredNamespaces[$name];
        }
        throw new BadMethodCallException("Namespace [$name] not found");
    }

    /**
     * {@inheritdoc}
     */
    public function extractArgument(&$params, $arg)
    {
        if (is_object($params) === true) {
            $params = (array) $params;
        }

        if (array_key_exists($arg, $params) === true) {
            $val = $params[$arg];
            unset($params[$arg]);

            return $val;
        } else {
            return null;
        }
    }

    private function verifyNotNullOrEmpty($name, $var)
    {
        if ($var === null) {
            throw new InvalidArgumentException("$name cannot be null.");
        }

        if (is_string($var)) {
            if (strlen($var) === 0) {
                throw new InvalidArgumentException("$name cannot be an empty string");
            }
        }

        if (is_array($var)) {
            if (strlen(implode("", $var)) === 0) {
                throw new InvalidArgumentException("$name cannot be an array of empty strings");
            }
        }
    }

    /**
     * @param $endpoint AbstractEndpoint
     *
     * @throws \Exception
     * @return array
     */
    private function performRequest(AbstractEndpoint $endpoint)
    {
        $promise =  $this->transport->performRequest(
            $endpoint->getMethod(),
            $endpoint->getURI(),
            $endpoint->getParams(),
            $endpoint->getBody(),
            $endpoint->getOptions()
        );

        return $this->transport->resultOrFuture($promise, $endpoint->getOptions());
    }
}
