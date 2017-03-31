package ch.daplab.bitcoin;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bitcoinj.core.*;
import org.bitcoinj.core.listeners.OnTransactionBroadcastListener;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.store.BlockStore;
import org.bitcoinj.store.MemoryBlockStore;
import org.bitcoinj.utils.BriefLogFormatter;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Created by bperroud on 30/03/17.
 */
public class TxListener implements OnTransactionBroadcastListener, Closeable {

    public static final String TOPIC = "bitcoin_transactions";
    public static final String BITCOIN_NODE = "213.32.30.232";

    public static final String TEST_HASH = "000000000000000000eea02beb8e565d1e80e0012253681c39e416b86f358c5a";

    KafkaProducer<String, byte[]> producer;
    final BitcoinSerializer bs;

    public static void main(String[] args) throws Exception {
        BriefLogFormatter.init();
        System.out.println("Connecting to node");
        final NetworkParameters params = MainNetParams.get();

        InetAddress address = InetAddress.getByName(BITCOIN_NODE);
        BlockStore blockStore = new MemoryBlockStore(params);
        BlockChain chain = new BlockChain(params, blockStore);
        Context c = new Context(params);

        chain.addNewBestBlockListener(block -> {
            System.out.println("NewBestBlockListener :" + block);
        });

        PeerGroup peerGroup = new PeerGroup(params, chain);
        peerGroup.setFastCatchupTimeSecs(System.currentTimeMillis());
        peerGroup.start();

        PeerAddress addr = new PeerAddress(params, address);
        peerGroup.addAddress(addr);
        peerGroup.waitForPeers(1).get();
        final Peer peer = peerGroup.getConnectedPeers().get(0);

        final TxListener txListener = new TxListener("daplab-rt-11.fri.lan:6667,daplab-rt-12.fri.lan:6667,daplab-rt-13.fri.lan:6667,daplab-rt-14.fri.lan:6667", params);

        peerGroup.addOnTransactionBroadcastListener(txListener);

        Sha256Hash blockHash = Sha256Hash.wrap(TEST_HASH);
        Future<Block> future = peer.getBlock(blockHash);

        System.out.println("Waiting for node to send us the requested block: " + blockHash);
        Block block = future.get();
        System.out.println("Got the block " + block.getHash());

        System.out.println("Best height from the peer: " + peer.getBestHeight());
        System.out.println("Most common height seen in the peer group: " + peerGroup.getMostCommonChainHeight());

        ListenableFuture<Long> ping = peer.ping();
        System.out.println("Got the ping: " + ping.get());

        System.in.read();

        peerGroup.stopAsync();
        txListener.close();
    }


    public TxListener(String bootstrapServers, NetworkParameters networkParameters) {
        this.bs = new BitcoinSerializer(networkParameters, false);
        producer = initKafkaProducer(bootstrapServers);
    }

    protected KafkaProducer initKafkaProducer(String bootstrapServers) {

        Map<String, String> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return new KafkaProducer(props);
    }

    @Override
    public void onTransaction(Peer peer, Transaction t) {

        System.out.println("Got a TX " + t.getHash() + " from " + peer);
        try {
            // Serialize the transaction back to wire protocol.
            ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
            bs.serialize(t, baos);
            byte[] payload = baos.toByteArray();
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, payload);
            producer.send(record);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
