#!/usr/bin/env python3

import sys
import threading
import re

from kafka import KafkaProducer, KafkaConsumer


should_quit = False


def read_messages(consumer):
    # TODO À compléter
    while not should_quit:
        # On utilise poll pour ne pas bloquer indéfiniment quand should_quit
        # devient True
        received = consumer.poll(100)

        for channel, messages in received.items():
            for msg in messages:
                print("< %s: %s" % (channel.topic, msg.value))



def cmd_msg(producer, channel, line):
    pass


def cmd_join(consumer, producer, line):
    channel_ok = re.match(r'^#[a-zA-Z0-9_-]+$',line)
    if not channel_ok:
        print("ERROR: " + line + " as a channel name is not handled")
        return False
    else:
        consumer.subscribe("chat_channel_" + line[1:])
        return True

def cmd_part(consumer, producer, line):
    # TODO À compléter
    pass



def cmd_quit(producer, line):
    # TODO À compléter
    pass



def main_loop(nick, consumer, producer):
    curchan = None

    while True:
        try:
            if curchan is None:
                line = input("> ")
            else:
                line = input("[#%s]> " % curchan)
        except EOFError:
            print("/quit")
            line = "/quit"

        if line.startswith("/"):
            cmd, *args = line[1:].split(" ", maxsplit=1)
            cmd = cmd.lower()
            args = None if args == [] else args[0]
        else:
            cmd = "msg"
            args = line

        if cmd == "msg":
            cmd_msg(producer, curchan, args)
        elif cmd == "join":
            if cmd_join(consumer, producer, args):
                curchan = args
        elif cmd == "part":
            cmd_part(consumer, producer, args)
        elif cmd == "quit":
            cmd_quit(producer, args)
            break
        # TODO: rajouter des commandes ici



def main():
    if len(sys.argv) != 2:
        print("usage: %s nick" % sys.argv[0])
        return 1

    nick = sys.argv[1]
    consumer = KafkaConsumer() ## par defaut il se met sur le localhost 9092
    producer = KafkaProducer()
    th = threading.Thread(target=read_messages, args=(consumer,))
    th.start()

    try:
        main_loop(nick, consumer, producer)
    finally:
        global should_quit
        should_quit = True
        th.join()



if __name__ == "__main__":
    sys.exit(main())
