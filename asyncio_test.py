import time
import asyncio


def is_prime(x):
    return not any(x // i == x / i for i in range(x - 1, 1, -1))


async def highest_prime_below(x):
    # print(threading.current_thread())
    print('Highest prime below %d' % x)
    for y in range(x - 1, 0, -1):

        if is_prime(y):
            print('→ Highest prime below %d is %d' % (x, y))
            return y
        await asyncio.sleep(0.01)

    return y



from agent import adding,send_value,Add

while True:
    loop = asyncio.get_event_loop()

    res1 = loop.run_until_complete(highest_prime_below(10000))

    #res2 = loop.run_until_complete(highest_prime_below(1000))
    res2 = loop.run_until_complete(send_value())

    print(res1,res2)

# loop .new_event_loop()
# while True:
#     #loop = asyncio.new_event_loop()
#     asyncio.ensure_future(highest_prime_below(10000))
#     asyncio.ensure_future(send_value())
#     loop.run_forever()
#
#     loop.close()
#     print(rez1,rez2)

