package ch.daplab.bitcoin;

import com.google.common.util.concurrent.ListenableFuture;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.store.BlockStore;
import org.bitcoinj.store.MemoryBlockStore;
import org.bitcoinj.utils.BriefLogFormatter;

import java.net.InetAddress;
import java.util.concurrent.Future;

/**
 * Created by bperroud on 30/03/17.
 */
public class TxListener {

    static String hash = "000000000000000000eea02beb8e565d1e80e0012253681c39e416b86f358c5a";

    public static void main(String[] args) throws Exception {
        BriefLogFormatter.init();
        System.out.println("Connecting to node");
        final NetworkParameters params = MainNetParams.get();

        InetAddress address = InetAddress.getByName("213.32.30.232");

//        BlockStore blockStore = new SPVBlockStore(params, Files.createTempFile("blockstore", ".dat").toFile());
        BlockStore blockStore = new MemoryBlockStore(params);

//        CheckpointManager
        BlockChain chain = new BlockChain(params, blockStore);
        Context c = new Context(params);

        chain.addNewBestBlockListener(block -> {
            System.out.println("NewBestBlockListener :" + block);
//            block.get
        });
//        chain.addTransactionReceivedListener(command -> {
//
//        });
        PeerGroup peerGroup = new PeerGroup(params, chain);
        peerGroup.setFastCatchupTimeSecs(System.currentTimeMillis());
        peerGroup.start();

        PeerAddress addr = new PeerAddress(params, address);
        peerGroup.addAddress(addr);
        peerGroup.waitForPeers(1).get();
        Peer peer = peerGroup.getConnectedPeers().get(0);

//        chain.d
        peerGroup.addOnTransactionBroadcastListener((peer1, t) ->
        {
            System.out.println("TX from peer " + peer1.toString() + ": " + t);
        });
//        peerGroup.addBlocksDownloadedEventListener((peer1, block, filteredBlock, blocksLeft) -> {
//            block.getH
//            System.out.println("Block from peer " + peer1.toString() + ", " + blocksLeft + ": " + block + filteredBlock);
//        });

        Sha256Hash blockHash = Sha256Hash.wrap(hash);
        Future<Block> future = peer.getBlock(blockHash);


        System.out.println("Waiting for node to send us the requested block: " + blockHash);
        Block block = future.get();
        System.out.println(block.getPrevBlockHash());
        System.out.println(block.getTransactions().size());

        System.out.println(peer.getBestHeight());
        ListenableFuture<Long> ping = peer.ping();
        Long aLong = ping.get();

//        peer.

        System.out.println(peerGroup.getMostCommonChainHeight());

        System.in.read();

        peerGroup.stopAsync();
    }
}
