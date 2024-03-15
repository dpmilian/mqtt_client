/*
 * Package : mqtt_client
 * Author : S. Hamblett <steve.hamblett@linux.com>
 * Date   : 22/06/2017
 * Copyright :  S.Hamblett
 */

part of '../../../mqtt_server_client.dart';

/// The MQTT normal(insecure TCP) server connection class
class MqttServerNormalConnection extends MqttServerConnection<Socket> {
  /// Default constructor
  MqttServerNormalConnection(super.eventBus, super.socketOptions);

  /// Initializes a new instance of the MqttConnection class.
  MqttServerNormalConnection.fromConnect(String server, int port,
      events.EventBus eventBus, List<RawSocketOption> socketOptions)
      : super(eventBus, socketOptions) {
    connect(server, port);
  }

  /// Connect
  @override
  Future<Socket?> connect(String server, int port) async {
    final completer = Completer<Socket>();
    MqttLogger.log('MqttNormalConnection::connect - entered');
    try {
      // Connect and save the socket.
      final socket = await Socket.connect(server, port);
      final applied = _applySocketOptions(socket, socketOptions);
      if (applied) {
        MqttLogger.log(
            'MqttNormalConnection::connect - socket options applied');
      }
      client = socket;
      readWrapper = ReadWrapper();
      messageStream = MqttByteBuffer(typed.Uint8Buffer());
      _startListening();
      completer.complete(socket);

      // final done = await socket.done;
      // MqttLogger.log('SOCKET DONE: $done');
      // completer.completeError('The SOCKET IS DONE $done');
      unawaited(socket.done.catchError((error) {
        MqttLogger.log('Socket.done error $error');
        // completer.completeError(error);
      }));

      return completer.future;

      // Socket options
    } on SocketException catch (e) {
      onError(e);
      final message =
          'MqttNormalConnection::connect - The connection to the message broker '
          '{$server}:{$port} could not be made. Error is ${e.toString()}';
      throw NoConnectionException(message);
    } on Exception catch (e) {
      onError(e);
      final message =
          'MqttNormalConnection::Connect - The connection to the message '
          'broker {$server}:{$port} could not be made.';
      throw NoConnectionException(message);
    }
  }

  /// Connect Auto
  @override
  Future<Socket> connectAuto(String server, int port) async {
    MqttLogger.log('MqttNormalConnection::connectAuto - entered');
    try {
      // Connect and save the socket.
      final socket = await Socket.connect(server, port);
      unawaited(socket.done.then<Socket>((done) {
        MqttLogger.log('SOCKET DONE: $done');
        return socket;
      }).catchError((e) {
        MqttLogger.log(
            'MqttNormalConnection::connect - error during socket lifecycle: $e');
        return socket;
      }));

      // Socket options
      final applied = _applySocketOptions(socket, socketOptions);
      if (applied) {
        MqttLogger.log(
            'MqttNormalConnection::connectAuto - socket options applied');
      }
      client = socket;
      _startListening();
      return socket;
    } on SocketException catch (e) {
      final message =
          'MqttNormalConnection::connectAuto - The connection to the message broker '
          '{$server}:{$port} could not be made. Error is ${e.toString()}';
      onError(e);
      throw NoConnectionException(message);
    } on Exception catch (e) {
      onError(e);
      final message =
          'MqttNormalConnection::ConnectAuto - The connection to the message '
          'broker {$server}:{$port} could not be made.';
      throw NoConnectionException(message);
    }
  }

  /// Sends the message in the stream to the broker.
  @override
  void send(MqttByteBuffer message) {
    final messageBytes = message.read(message.length);
    client?.add(messageBytes.toList());
  }

  /// Stops listening the socket immediately.
  @override
  void stopListening() {
    for (final listener in listeners) {
      listener.cancel();
    }

    listeners.clear();
  }

  /// Closes the socket immediately.
  @override
  void closeClient() {
    client?.destroy();
    client?.close();
  }

  /// Closes and dispose the socket immediately.
  @override
  void disposeClient() {
    closeClient();
    if (client != null) {
      client = null;
    }
  }

  /// Implement stream subscription
  @override
  StreamSubscription onListen() {
    final socket = client;
    if (socket == null) {
      throw StateError('socket is null');
    }

    return socket.listen(onData, onError: onError, onDone: onDone);
  }
}
