defmodule KafkaEx.AbstractSocket.Test do
  use ExUnit.Case, async: false

  defmodule SSLServer do
    def start(port) do
      {:ok, listen_socket} = :ssl.listen(port, [:binary, {:active, false}, {:reuseaddr, true}, {:packet, 0}, {:certfile, 'test/fixtures/server.crt'}, {:keyfile, 'test/fixtures/server.key'}])
      spawn_link(fn -> listen(listen_socket) end)
    end

    defp listen(socket) do
       case :ssl.transport_accept(socket) do
        {:ok, conn} ->
          :ok = :ssl.ssl_accept(conn)
          pid = spawn_link(fn -> recv(conn) end)
          :ssl.controlling_process(socket, pid)
        _ -> :ok
      end
      listen(socket)
    end

    defp recv(conn) do
      case :ssl.recv(conn, 0) do
        {:ok, data} ->
          :ok = :ssl.send(conn, data)
        {:error, :closed} ->
          :ok
      end
    end
  end

  defmodule Server do
    def start(port) do
      {:ok, listen_socket} = :gen_tcp.listen(port, [:binary, {:active, false}, {:reuseaddr, true}, {:packet, 0}])
      spawn_link(fn -> listen(listen_socket) end)
    end

    defp listen(socket) do
      {:ok, conn} = :gen_tcp.accept(socket)
      spawn_link(fn -> recv(conn) end)
      listen(socket)
    end

    defp recv(conn) do
      case :gen_tcp.recv(conn, 0) do
        {:ok, data} ->
          :ok = :gen_tcp.send(conn, data)
        {:error, :closed} ->
          :ok
      end
    end
  end

  setup_all do
    :ssl.start
    SSLServer.start(3030)
    Server.start(3040)
    {:ok, [ssl_port: 3030, port: 3040]}
  end

  describe "SSL sockets" do
    test "create socket", context do
      {:ok, socket} = KafkaEx.AbstractSocket.create('localhost', context[:ssl_port], [:ssl, :binary, {:packet, 0}])
      assert socket.ssl == true
      KafkaEx.AbstractSocket.close(socket)
    end

    test "send and receive", context do
      {:ok, socket} = KafkaEx.AbstractSocket.create('localhost', context[:ssl_port], [:ssl, :binary, {:packet, 0}, {:active, false}])
      KafkaEx.AbstractSocket.send(socket, 'ping')
      assert {:ok, "ping" } == KafkaEx.AbstractSocket.recv(socket, 0)
      KafkaEx.AbstractSocket.close(socket)
    end

    test "info", context do
      {:ok, socket} = KafkaEx.AbstractSocket.create('localhost', context[:ssl_port], [:ssl, :binary, {:packet, 0}, {:active, false}])
      info = KafkaEx.AbstractSocket.info(socket)
      assert info[:name] == 'tcp_inet'
      KafkaEx.AbstractSocket.close(socket)
      assert {:error, :closed} == KafkaEx.AbstractSocket.send(socket, 'ping')
    end
  end

  describe "non SSL sockets" do
    test "create socket", context do
      {:ok, socket} = KafkaEx.AbstractSocket.create('localhost', context[:port], [:binary, {:packet, 0}])
      assert socket.ssl == false
      KafkaEx.AbstractSocket.close(socket)
    end

    test "send and receive", context do
      {:ok, socket} = KafkaEx.AbstractSocket.create('localhost', context[:port], [:binary, {:packet, 0}, {:active, false}])
      KafkaEx.AbstractSocket.send(socket, 'ping')
      assert {:ok, "ping" } == KafkaEx.AbstractSocket.recv(socket, 0)
      KafkaEx.AbstractSocket.close(socket)
    end

    test "info", context do
      {:ok, socket} = KafkaEx.AbstractSocket.create('localhost', context[:port], [:binary, {:packet, 0}, {:active, false}])
      info = KafkaEx.AbstractSocket.info(socket)
      assert info[:name] == 'tcp_inet'
      KafkaEx.AbstractSocket.close(socket)
      assert {:error, :closed} == KafkaEx.AbstractSocket.send(socket, 'ping')
    end
  end
end
