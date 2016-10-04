defmodule KafkaEx.AbstractSocket do
  @moduledoc false

  defmodule Socket do
    @moduledoc false
    defstruct socket: nil, ssl: false
    @type t :: %Socket{socket: :gen_tcp.socket | :ssl.sslsocket, ssl: boolean}
  end

  @spec create(string, non_neg_integer, [] | [...]) :: {:ok, Socket.t} | {:error, any}
  def create(host, port, socket_options \\ []) do
    {is_ssl, new_options} = extract_ssl_option(socket_options)
    case create_socket(host, port, is_ssl, new_options) do
      {:ok, socket} -> {:ok, %Socket{socket: socket, ssl: is_ssl}}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec close(Socket.t) :: :ok
  def close(%Socket{ssl: true} = socket), do: :ssl.close(socket.socket)
  def close(socket), do: :gen_tcp.close(socket.socket)

  @spec send(Socket.t, iodata) :: :ok | {:error, any}
  def send(%Socket{ssl: true} = socket, data) do
    :ssl.send(socket.socket, data)
  end
  def send(socket, data) do
    :gen_tcp.send(socket.socket, data)
  end

  @spec setopts(Socket.t, list) :: :ok | {:error, any}
  def setopts(%Socket{ssl: true} = socket, options) do
    :ssl.setopts(socket.socket, options)
  end
  def setopts(socket, options) do
    :inet.setopts(socket.socket, options)
  end

  @spec recv(Socket.t, non_neg_integer) :: {:ok, String.t | binary | term} | {:error, any}
  def recv(%Socket{ssl: true} = socket, length) do
    :ssl.recv(socket.socket, length)
  end
  def recv(socket, length) do
    :gen_tcp.recv(socket.socket, length)
  end

  @spec recv(Socket.t, non_neg_integer, timeout) :: {:ok, String.t | binary | term} | {:error, any}
  def recv(%Socket{ssl: true} = socket, length, timeout) do
    :ssl.recv(socket.socket, length, timeout)
  end
  def recv(socket, length, timeout) do
    :gen_tcp.recv(socket.socket, length, timeout)
  end

  @spec info(Socket.t | nil) :: list | nil
  def info(socket) do
    socket
      |> extract_port
      |> Port.info
  end

  defp extract_port(%Socket{ssl: true} = socket) do
    {:sslsocket, socket_info, _} = socket.socket
    socket_info
      |> elem(1)
  end
  defp extract_port(socket), do: socket.socket

  defp create_socket(host, port, ssl, socket_options) do
    if ssl do
      :ssl.connect(host, port, socket_options)
    else
      :gen_tcp.connect(host, port, socket_options)
    end
  end

  defp extract_ssl_option(socket_options) do
    is_ssl = Enum.any?(socket_options, &(&1 == :ssl))
    new_options = List.delete(socket_options, :ssl)
    {is_ssl, new_options}
  end
end
