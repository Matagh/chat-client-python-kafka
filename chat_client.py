#!/usr/bin/env python3

import sys
import threading
import re

from kafka import KafkaProducer, KafkaConsumer


should_quit = False
LIST_CHAN_SUB = []

def read_messages(consumer):
    # TODO À compléter
    while not should_quit:
        # On utilise poll pour ne pas bloquer indéfiniment quand should_quit
        # devient True
        received = consumer.poll()

        for channel, messages in received.items():
            for msg in messages:
                str_msg = str(msg.value, encoding='utf-8')
                print("< %s: %s" % (channel.topic, str_msg))
        



def cmd_msg(username, producer, channel, line):
    if(channel == None):
        raise ValueError("Can't send your message, you are not in a channel")
    else:
        producer.send(chan_to_topic(channel), bytes('<' + username + '> '+line, 'utf-8'))


def cmd_join(consumer, producer, line):
    channel_ok = re.match(r'^#[a-zA-Z0-9_-]+$',line)
    if not channel_ok:
        print("ERROR: " + line + " as a channel name is not handled")
        return False
    else:
        LIST_CHAN_SUB.append(line)
        new_list_topic = []
        for chan in LIST_CHAN_SUB:
            new_list_topic.append(chan_to_topic(chan))
        consumer.subscribe(new_list_topic)
        print(consumer.subscription())
        return True

def cmd_part(consumer, producer, line):
    channel_unsub_ok = re.match(r'^#[a-zA-Z0-9_-]+$',line)
    if not channel_unsub_ok:
        raise ValueError("ERROR: " + line + " as a channel name is not handled")
    if line in LIST_CHAN_SUB:
        LIST_CHAN_SUB.remove(line)
        if len(LIST_CHAN_SUB) < 1:
            consumer.unsubscribe()
            print("You are not subscribe a channel anymore")
            return None
        else:
            temp_list_topic = []
            for chan in LIST_CHAN_SUB:
                temp_list_topic.append(chan_to_topic(chan))
            consumer.subscribe(temp_list_topic)
            print(consumer.subscription())
            return LIST_CHAN_SUB[0]
    else:
        raise ValueError("ERROR: " + line + " is not part of your subscribed channel") 

def cmd_active(line, active_chan_current):
    channel_unsub_ok = re.match(r'^#[a-zA-Z0-9_-]+$',line)
    if not channel_unsub_ok:
        raise ValueError("ERROR: " + line + " as a channel name is not handled")
    if not line in LIST_CHAN_SUB:
        print("You are not yet subscribe to this channel, join (/join) the channel to make it your active channel")
        return active_chan_current
    if line == active_chan_current:
        print(line + ": this channel is already your active channel")
        return active_chan_current
    else:
        return line
    pass

# cette méthode permet de transformer les noms d'une liste de channel en liste de nom standard pour le topic (de type 'chat_channel_[nomchannel]')
def chan_to_topic(chan):
    return "chat_channel_" + chan[1:]

def cmd_quit(producer, line):
    # TODO À compléter
    pass



def main_loop(username, consumer, producer):
    curchan = None
    LIST_CHAN_SUB=[]

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
            cmd_msg(username, producer, curchan, args)
        elif cmd == "join":
            if cmd_join(consumer, producer, args):
                curchan = args
                LIST_CHAN_SUB.append(args)
            print(consumer.topics())
        elif cmd == "part":
            temp = cmd_part(consumer, producer, args)
            if (temp or temp == None):
                curchan = temp
        elif cmd == "active":
            curchan = cmd_active(args,curchan)
        elif cmd == "quit":
            cmd_quit(producer, args)
            break

def main():
    if len(sys.argv) != 2:
        print("usage: %s username" % sys.argv[0])
        return 1

    username = sys.argv[1]
    consumer = KafkaConsumer() ## par defaut il se met sur le localhost 9092
    producer = KafkaProducer()
    th = threading.Thread(target=read_messages, args=(consumer,))
    th.start()

    try:
        main_loop(username, consumer, producer)
    finally:
        global should_quit
        should_quit = True
        th.join()



if __name__ == "__main__":
    sys.exit(main())
