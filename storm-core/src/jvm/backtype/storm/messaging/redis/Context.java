package backtype.storm.messaging.redis;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import redis.clients.jedis.Jedis;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Map;

/**
 * Created by ding on 14-8-2.
 */
public class Context implements IContext {

    @Override
    public void prepare(Map storm_conf) {

    }

    @Override
    public void term() {

    }

    @Override
    public IConnection bind(String storm_id, int port) {
        String hostIp;
        try {
            hostIp = getRealIp();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String queue = storm_id + "_" + hostIp + "_" + port;
        System.out.println("Bind to queue " + queue);
        return new ReidsConnection(queue.getBytes(), new Jedis("192.168.0.30", 6379));
    }

    public static String getRealIp() throws SocketException {
        String localip = null;// 本地IP，如果没有配置外网IP则返回它
        String netip = null;// 外网IP

        Enumeration<NetworkInterface> netInterfaces =
                NetworkInterface.getNetworkInterfaces();
        InetAddress ip = null;
        boolean finded = false;// 是否找到外网IP
        while (netInterfaces.hasMoreElements() && !finded) {
            NetworkInterface ni = netInterfaces.nextElement();
            Enumeration<InetAddress> address = ni.getInetAddresses();
            while (address.hasMoreElements()) {
                ip = address.nextElement();
                if (!ip.isSiteLocalAddress()
                        && !ip.isLoopbackAddress()
                        && ip.getHostAddress().indexOf(":") == -1) {// 外网IP
                    netip = ip.getHostAddress();
                    finded = true;
                    break;
                } else if (ip.isSiteLocalAddress()
                        && !ip.isLoopbackAddress()
                        && ip.getHostAddress().indexOf(":") == -1) {// 内网IP
                    localip = ip.getHostAddress();
                }
            }
        }
        if (netip != null && !"".equals(netip)) {
            return netip;
        } else {
            return localip;
        }
    }

    @Override
    public IConnection connect(String storm_id, String host, int port) {
        String hostIp;
        try {
            hostIp = InetAddress.getByName(host).getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        String queue = storm_id + "_" + hostIp + "_" + port;
        System.out.println("Connect to queue " + queue);
        return new ReidsConnection(queue.getBytes(), new Jedis("192.168.0.30", 6379));
    }

}
