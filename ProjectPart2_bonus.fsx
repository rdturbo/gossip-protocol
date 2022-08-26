#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit"

open System
open System.Collections.Generic
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

let timer = Diagnostics.Stopwatch.StartNew()
let system = ActorSystem.Create("FSharp")
let threeDimensionDims totalNumber =
      let mutable dimension = 0.0
      let tempDimension = System.Math.Cbrt(totalNumber)
      if (tempDimension * 100.0 % 100.0) < 50.0 then
         dimension <- System.Math.Floor(tempDimension)
      else 
         dimension <- System.Math.Ceiling(tempDimension)
      dimension, tempDimension
      
let threeDimension dimension (tempDimension:float) (actorID:float) (totalNumber:float) = 
      let mutable nlist = []
      let mutable curRow = 0.0
      let mutable curCol = 0.0
      let row = dimension
      let col = dimension
      let gridPhase = System.Math.Ceiling(actorID / (dimension * dimension))
      let tempCurRow = ((actorID - (((gridPhase - 1.0)*row*row)-1.0))/row) 
      if (tempCurRow * 100.0 % 100.0) < 50.0 then
         curRow <- System.Math.Floor(tempCurRow)
      else
         curRow <- System.Math.Ceiling(tempCurRow)

      let tempcurCol = actorID % col

      if tempcurCol = 0.0 then
         curCol <- col
      else 
         curCol <- tempcurCol

      if gridPhase = 1.0 then
         nlist <- actorID + (row * row) :: nlist
      else if gridPhase = row && System.Math.Ceiling(tempDimension) <= gridPhase then
         nlist <- actorID - (row * row) :: nlist
      else 
         nlist <- actorID - (row * row) :: nlist
         nlist <- actorID + (row * row) :: nlist

      if curRow = 1.0 then
         nlist <- actorID + row :: nlist
      else if curRow = row then
         nlist <- actorID - row :: nlist
      else
         nlist <- actorID + row :: nlist
         nlist <- actorID - row :: nlist

      if curCol = 1.0 then
         nlist <- actorID + 1.0 :: nlist
      else if curCol = col then
         nlist <- actorID - 1.0 :: nlist
      else
         nlist <- actorID + 1.0 :: nlist
         nlist <- actorID - 1.0 :: nlist         
      let neighborsList = [for item in nlist do if item <= totalNumber && item >= 1.0 then int(item)]
      neighborsList
let AssignNeighbour neighbourList =
    let mutable neigh1, neigh2, neigh3, neigh4, neigh5, neigh6 = -1, -1, -1, -1, -1, -1
    for item in neighbourList do
        if neigh1 = -1 then
            neigh1 <- item
        else if neigh2 = -1 then
            neigh2 <- item
        else if neigh3 = -1 then
            neigh3 <- item
        else if neigh4 = -1 then
            neigh4 <- item
        else if neigh5 = -1 then
            neigh5 <- item
        else
            neigh6 <- item
    neigh1, neigh2, neigh3, neigh4, neigh5, neigh6
type DispatcherMessageImpe3D = int * int * int * int * int * int * int * int * int
type DispatcherMessageImpe3DSum = int * int * int * int * int * int * int * int * int * float * float
type DispatcherMessage3D = int * int * int * int * int * int * int * int
type DispatcherMessage3DSum = int * int * int * int * int * int * int * int * float * float
type DispatcherMessageLine = int * int
type DispatcherMessageLineSum = int * int * float * float
type DispatcherMessageFull = int * int
type DispatcherMessageFullSum = int * int * float * float
type SizeAndID = int * int
type SumWeight = float * float * int * int
type DispacherMessageWithOffCount = int * int

type MessagesOfActor = 
    | Finished of string
    | WakeTheNode of int
    | DispatcherInput of DispacherMessageWithOffCount
    | MessageToWorkerFull of DispatcherMessageFull
    | MessageToWorkerFullSum of DispatcherMessageFullSum
    | MessageToWorkerLine of DispatcherMessageLine
    | MessageToWorkerLineSum of DispatcherMessageLineSum
    | MessageToWorker3D of DispatcherMessage3D
    | MessageToWorker3DSum of DispatcherMessage3DSum
    | MessageToWorkerImpe3D of DispatcherMessageImpe3D
    | MessageToWorkerImpe3DSum of DispatcherMessageImpe3DSum
    | MessageToNode of SizeAndID
    | MessageToNodeSum of SumWeight
    | ShutdownMessage of string

let randGossip = Random()
let mutable countResponce = 0 
let mutable globalWorkerList = new List<IActorRef>()
let mutable globalWorkerDoneList = new List<int>()

let workerImpe3D(mailbox: Actor<_>) = 
                let mutable messageRecivedCount = 0
                let mutable dispatcher = mailbox.Self
                let mutable numNodes = 0
                let mutable ID = -100
                let mutable  neigh1, neigh2, neigh3, neigh4, neigh5, neigh6, neigh7 = -1, -1, -1, -1, -1, -1, -1
                let mutable recivedShutdown = 0
                let rec loop() = actor {
                        mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 1000.0)
                        let! message = mailbox.Receive()
                        if dispatcher = mailbox.Self then 
                            dispatcher <- mailbox.Sender()
                        match message with
                        | MessageToWorkerImpe3D(nodNum, nodeID, nig1, nig2, nig3, nig4, nig5, nig6, nig7) -> 
                            // printfn "I am an actor and I have this message (%d, %d)\n" nodNum nodeID
                            numNodes <- nodNum
                            ID <- nodeID 
                            neigh1 <- nig1
                            neigh2 <- nig2
                            neigh3 <- nig3
                            neigh4 <- nig4
                            neigh5 <- nig5
                            neigh6 <- nig6
                            neigh7 <- nig7
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), mailbox.Self, WakeTheNode(1))
                        | MessageToNode(msg, senderID) ->
                            if recivedShutdown = 0 then
                                numNodes <- msg
                                messageRecivedCount <- messageRecivedCount + 1
                                if messageRecivedCount = 1 then
                                    dispatcher <! Finished(sprintf "Got %d messages!" (10))
                        | WakeTheNode(com) ->
                            if numNodes <> -100 && messageRecivedCount < 11 && recivedShutdown = 0 then
                                let mutable neighbour = [neigh1; neigh2; neigh3; neigh4; neigh5; neigh6; neigh7].[randGossip.Next() % 7] 
                                while neighbour = -1 do
                                    neighbour <- [neigh1; neigh2; neigh3; neigh4; neigh5; neigh6; neigh7].[randGossip.Next() % 7]                           
                                globalWorkerList.Item((neighbour - 1)) <! MessageToNode(numNodes, ID) 
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), mailbox.Self, WakeTheNode(1))
                        | ShutdownMessage(msg) ->
                            if recivedShutdown = 0 then
                                recivedShutdown <- 1
                                dispatcher <! Finished(sprintf "Got the shutdown message")
                        | _ ->  printfn "An error occured at worker %d" ID
                        return! loop()
                    }
                loop()

let DispatcherImpe3D(mailbox: Actor<_>) = 
            let mutable numNodes = 0
            let rec loop() = actor {
                    mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 10000.0)
                    let! message = mailbox.Receive()
                    match message with
                    | DispatcherInput(n, x) ->
                        numNodes <- n
                        for i in 1 .. numNodes do
                            globalWorkerList.Add(spawn system (sprintf "Worker_%d" i) workerImpe3D)
                        let dimension, tempDimension = threeDimensionDims(float(numNodes))
                        let nodeThattKnows = randGossip.Next(1, numNodes + 1)
                        let neighbourList = threeDimension dimension tempDimension (float(nodeThattKnows)) (float(numNodes))
                        let neigh1, neigh2, neigh3, neigh4, neigh5, neigh6 = AssignNeighbour neighbourList
                        for j in 0 .. x - 1 do
                            let mutable node = randGossip.Next(1, numNodes + 1)
                            while node = nodeThattKnows do
                                node <- randGossip.Next(1, numNodes + 1)
                            globalWorkerList.Item(node - 1) <! ShutdownMessage("Sutdown! Your work is done my child!")
                        let mutable neigh7 = randGossip.Next(1, n + 1)
                        while neigh7 = nodeThattKnows do
                            neigh7 <- randGossip.Next(1, n + 1)
                        globalWorkerList.Item(nodeThattKnows - 1) <! MessageToWorkerImpe3D((numNodes, nodeThattKnows, neigh1, neigh2, neigh3, neigh4, neigh5, neigh6, neigh7))
                        for j in 0 .. numNodes - 1 do
                            if j <> nodeThattKnows - 1 then
                                let neighbourList = threeDimension dimension tempDimension (float(j + 1)) (float(numNodes))
                                let neigh1, neigh2, neigh3, neigh4, neigh5, neigh6 = AssignNeighbour neighbourList
                                let mutable neigh7 = randGossip.Next(1, n + 1)
                                while neigh7 = j + 1 do
                                    neigh7 <- randGossip.Next(1, n + 1)
                                timer.Start()
                                globalWorkerList.Item(j) <! MessageToWorkerImpe3D((-100, j + 1, neigh1, neigh2, neigh3, neigh4, neigh5, neigh6, neigh7))
                    | Finished(output) ->
                        countResponce <- countResponce + 1
                        if countResponce <= numNodes then
                            // printfn "%s"output
                            if countResponce = numNodes then
                                printfn "Time taken = %i milliseconds\n" timer.ElapsedMilliseconds
                                mailbox.Context.System.Terminate() |> ignore
                    | _ -> printfn "An error occured at dipatcher"
                    return! loop()
                }
            loop()

let worker3D(mailbox: Actor<_>) = 
                let mutable messageRecivedCount = 0
                let mutable dispatcher = mailbox.Self
                let mutable numNodes = 0
                let mutable ID = -100
                let mutable  neigh1, neigh2, neigh3, neigh4, neigh5, neigh6 = -1, -1, -1, -1, -1, -1
                let mutable recivedShutdown = 0
                let rec loop() = actor {
                        mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 1000.0)
                        let! message = mailbox.Receive()
                        if dispatcher = mailbox.Self then 
                            dispatcher <- mailbox.Sender()
                        match message with
                        | MessageToWorker3D(nodNum, nodeID, nig1, nig2, nig3, nig4, nig5, nig6) -> 
                            // printfn "I am an actor and I have this message (%d, %d)\n" nodNum nodeID
                            numNodes <- nodNum
                            ID <- nodeID 
                            neigh1 <- nig1
                            neigh2 <- nig2
                            neigh3 <- nig3
                            neigh4 <- nig4
                            neigh5 <- nig5
                            neigh6 <- nig6
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), mailbox.Self, WakeTheNode(1))
                        | MessageToNode(msg, senderID) ->
                            if recivedShutdown = 0 then
                                numNodes <- msg
                                messageRecivedCount <- messageRecivedCount + 1
                                if messageRecivedCount = 1 then
                                    dispatcher <! Finished(sprintf "Got %d messages!" (10))
                        | WakeTheNode(com) ->
                            if numNodes <> -100 && messageRecivedCount < 11 && recivedShutdown = 0 then
                                let mutable neighbour = [neigh1; neigh2; neigh3; neigh4; neigh5; neigh6].[randGossip.Next() % 6] 
                                while neighbour = -1 do
                                    neighbour <- [neigh1; neigh2; neigh3; neigh4; neigh5; neigh6].[randGossip.Next() % 6]                          
                                globalWorkerList.Item((neighbour - 1)) <! MessageToNode(numNodes, ID) 
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), mailbox.Self, WakeTheNode(1))
                        | ShutdownMessage(msg) ->
                            if recivedShutdown = 0 then
                                recivedShutdown <- 1
                                dispatcher <! Finished(sprintf "Got the shutdown message")
                        | _ ->  printfn "An error occured at worker %d" ID
                        return! loop()
                    }
                loop()

let Dispatcher3D(mailbox: Actor<_>) = 
            let mutable numNodes = 0
            let rec loop() = actor {
                    mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 10000.0)
                    let! message = mailbox.Receive()
                    match message with
                    | DispatcherInput(n, x) ->
                        numNodes <- n
                        for i in 1 .. numNodes do
                            globalWorkerList.Add(spawn system (sprintf "Worker_%d" i) worker3D)
                        let dimension, tempDimension = threeDimensionDims(float(numNodes))
                        let nodeThattKnows = randGossip.Next(1, numNodes + 1)
                        let neighbourList = threeDimension dimension tempDimension (float(nodeThattKnows)) (float(numNodes))
                        let neigh1, neigh2, neigh3, neigh4, neigh5, neigh6 = AssignNeighbour neighbourList
                        for j in 0 .. x - 1 do
                            let mutable node = randGossip.Next(1, numNodes + 1)
                            while node = nodeThattKnows do
                                node <- randGossip.Next(1, numNodes + 1)
                            globalWorkerList.Item(node - 1) <! ShutdownMessage("Sutdown! Your work is done my child!")
                        globalWorkerList.Item(nodeThattKnows - 1) <! MessageToWorker3D((numNodes, nodeThattKnows, neigh1, neigh2, neigh3, neigh4, neigh5, neigh6))
                        for j in 0 .. numNodes - 1 do
                            if j <> nodeThattKnows - 1 then
                                let neighbourList = threeDimension dimension tempDimension (float(j + 1)) (float(numNodes))
                                let neigh1, neigh2, neigh3, neigh4, neigh5, neigh6 = AssignNeighbour neighbourList
                                timer.Start()
                                globalWorkerList.Item(j) <! MessageToWorker3D((-100, j + 1, neigh1, neigh2, neigh3, neigh4, neigh5, neigh6))
                    | Finished(output) ->
                        countResponce <- countResponce + 1
                        if countResponce <= numNodes then
                            // printfn "%s"output
                            if countResponce = numNodes then
                                printfn "Time taken = %i milliseconds\n" timer.ElapsedMilliseconds
                                mailbox.Context.System.Terminate() |> ignore
                    | _ -> printfn "An error occured at dipatcher"
                    return! loop()
                }
            loop()

let workerLine(mailbox: Actor<_>) = 
                let mutable messageRecivedCount = 0
                let mutable dispatcher = mailbox.Self
                let mutable numNodes = 0
                let mutable ID = -100
                let mutable messagedNeighbour = new List<int>()
                let mutable recivedShutdown = 0
                let rec loop() = actor {
                        mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 1000.0)
                        let! message = mailbox.Receive()
                        if dispatcher = mailbox.Self then 
                            dispatcher <- mailbox.Sender()
                        match message with
                        | MessageToWorkerLine(nodNum, nodeID) -> 
                            // printfn "I am an actor and I have this message (%d, %d)\n" nodNum nodeID
                            numNodes <- nodNum
                            ID <- nodeID
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(20.0), mailbox.Self, WakeTheNode(1))
                        | MessageToNode(msg, senderID) ->
                            if recivedShutdown = 0 then
                                numNodes <- msg
                                messageRecivedCount <- messageRecivedCount + 1
                                if messageRecivedCount = 1 then
                                    dispatcher <! Finished(sprintf "Got a message!")
                        | WakeTheNode(com) ->
                            if numNodes <> -100 && messageRecivedCount < 11 && recivedShutdown = 0 then
                                let mutable neighbour =  [1; -1].[randGossip.Next() % 2]
                                while (ID + neighbour = ID) || (ID + neighbour < 1) || (ID + neighbour > numNodes) do
                                    neighbour <- [1; -1].[randGossip.Next() % 2]                          
                                globalWorkerList.Item(ID + neighbour - 1) <! MessageToNode(numNodes, ID)
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(20.0), mailbox.Self, WakeTheNode(1))
                        | ShutdownMessage(msg) ->
                            if recivedShutdown = 0 then
                                recivedShutdown <- 1
                                dispatcher <! Finished(sprintf "Got the shutdown message")
                        | _ ->  printfn "An error occured at worker %d" ID  
                        return! loop()
                    }
                loop()

let DispatcherLine(mailbox: Actor<_>) = 
            let mutable numNodes = 0
            let rec loop() = actor {
                    mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 10000.0)
                    let! message = mailbox.Receive()
                    match message with
                    | DispatcherInput(n, x) ->
                        numNodes <- n
                        timer.Start()
                        for i in 1 .. numNodes do
                            globalWorkerList.Add(spawn system (sprintf "Worker_%d" i) workerLine)
                        let neighbour = randGossip.Next(1, numNodes + 1)
                        for j in 0 .. x - 1 do
                            let mutable node = randGossip.Next(1, numNodes + 1)
                            while node = neighbour do
                                node <- randGossip.Next(1, numNodes + 1)
                            globalWorkerList.Item(node - 1) <! ShutdownMessage("Sutdown! Your work is done my child!")
                        globalWorkerList.Item(neighbour - 1) <! MessageToWorkerLine((numNodes, neighbour))
                        for j in 0 .. numNodes - 1 do
                            if j <> neighbour - 1 then 
                                globalWorkerList.Item(j) <! MessageToWorkerLine((-100, j + 1))
                    | Finished(output) ->
                        countResponce <- countResponce + 1
                        if countResponce <= numNodes then
                            printfn "%d"countResponce
                            if countResponce = numNodes then
                                printfn "Time taken = %i milliseconds\n" timer.ElapsedMilliseconds
                                mailbox.Context.System.Terminate() |> ignore
                    | _ -> printfn "An error occured at dipatcher"
                    return! loop()
                }
            loop()

let workerFull(mailbox: Actor<_>) = 
                let mutable messageRecivedCount = 0
                let mutable dispatcher = mailbox.Self
                let mutable numNodes = 0
                let mutable ID = -100
                let mutable messagedNeighbour = new List<int>()
                let mutable recivedShutdown = 0
                let rec loop() = actor {
                        mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 1000.0)
                        let! message = mailbox.Receive()
                        if dispatcher = mailbox.Self then 
                            dispatcher <- mailbox.Sender()
                        match message with
                        | MessageToWorkerFull(nodNum, nodeID) -> 
                            // printfn "I am an actor and I have this message (%d, %d)\n" nodNum nodeID
                            numNodes <- nodNum
                            ID <- nodeID
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), mailbox.Self, WakeTheNode(1))
                        | MessageToNode(msg, senderID) ->
                            if recivedShutdown = 0 then
                                numNodes <- msg
                                messageRecivedCount <- messageRecivedCount + 1
                                if messageRecivedCount = 1 then
                                    dispatcher <! Finished(sprintf "Heard the gossip")
                        | WakeTheNode(com) ->
                            if numNodes <> -100 && messageRecivedCount < 10 && recivedShutdown = 0 then
                                let mutable neighbour = randGossip.Next(1, numNodes + 1)
                                while neighbour = ID do
                                    neighbour <- randGossip.Next(1, numNodes + 1)                          
                                globalWorkerList.Item(neighbour - 1) <! MessageToNode(numNodes, ID)
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), mailbox.Self, WakeTheNode(1))
                        | ShutdownMessage(msg) ->
                            if recivedShutdown = 0 then
                                recivedShutdown <- 1
                                dispatcher <! Finished(sprintf "Got the shutdown message")
                        | _ ->  printfn "An error occured at worker %d" ID    
                        return! loop()
                    }
                loop()

let DispatcherFull(mailbox: Actor<_>) = 
            let mutable numNodes = 0
            let mutable wakeUpCount = 0
            let rec loop() = actor {
                    mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 10000.0)
                    let! message = mailbox.Receive()
                    match message with
                    | DispatcherInput(n, x) ->
                        numNodes <- n
                        for i in 1 .. numNodes do
                            globalWorkerList.Add(spawn system (sprintf "Worker_%d" i) workerFull)
                        let neighbour = randGossip.Next(1, numNodes + 1)
                        for j in 0 .. x - 1 do
                            let mutable node = randGossip.Next(1, numNodes + 1)
                            while node = neighbour do
                                node <- randGossip.Next(1, numNodes + 1)
                            globalWorkerList.Item(node - 1) <! ShutdownMessage("Sutdown! Your work is done my child!")
                        globalWorkerList.Item(neighbour - 1) <! MessageToWorkerFull((numNodes, neighbour))
                        for j in 0 .. numNodes - 1 do
                            if j <> neighbour - 1 then 
                                timer.Start()
                                globalWorkerList.Item(j) <! MessageToWorkerFull((-100, j + 1))                       
                    | Finished(output) ->
                        countResponce <- countResponce + 1
                        // printfn "Currently the number of terminated worker is  %d" countResponce
                        if countResponce <= numNodes then
                            // printfn "%s"output
                            if countResponce = numNodes then
                                printfn "Time taken = %i milliseconds\n" timer.ElapsedMilliseconds
                                mailbox.Context.System.Terminate() |> ignore
                    | _ -> printfn "An error occured at dipatcher"
                    return! loop()
                }
            loop()

let workerFullSum(mailbox: Actor<_>) = 
                let mutable dispatcher = mailbox.Self
                let mutable numNodes = 0
                let mutable ID = -100
                let mutable curSum = 0.0
                let mutable curWeight = 1.0 
                let mutable prevRatio = curSum / curWeight
                let mutable countRatioNotChanged = 0
                let mutable workerDone = false
                let mutable recivedShutdown = 0
                let mutable messagedNeighbour = new List<int>()
                let rec loop() = actor {
                        //mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 1000.0)
                        let! message = mailbox.Receive()
                        if dispatcher = mailbox.Self then 
                            dispatcher <- mailbox.Sender()
                        match message with
                        | MessageToWorkerFullSum(nodNum, nodeID, initSum, initWeight) -> 
                            // printfn "I am an actor and I have this message (%d, %d, %f, %f)\n" nodNum nodeID initSum initWeight
                            numNodes <- nodNum
                            ID <- nodeID
                            curSum <- initSum
                            curWeight <- initWeight
                            prevRatio <- curSum / curWeight
                            countRatioNotChanged <- 0
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(1.0), mailbox.Self, WakeTheNode(1))
                        | MessageToNodeSum(theSum, theWeight, msg, senderID) ->
                            if recivedShutdown = 0 then
                                numNodes <- msg
                                curSum <- curSum  + theSum
                                curWeight <- curWeight + theWeight
                                let newRatio = curSum / curWeight
                                // if randGossip.Next() % 10 = 9 then
                                // printfn "%f" newRatio 
                                if abs(prevRatio - newRatio) <= float(pown 10.0 -10) then
                                    countRatioNotChanged <- countRatioNotChanged + 1
                                    if countRatioNotChanged >= 3 && not workerDone then
                                        let ratioDiff = abs(prevRatio - newRatio)
                                        dispatcher <! Finished(sprintf "Got %d messages!" 10)
                                        workerDone <- true                                 
                                else 
                                    countRatioNotChanged <- 0
                                prevRatio <- curSum / curWeight
                        | WakeTheNode(com) ->
                            if numNodes <> -100 && recivedShutdown = 0 then
                                let mutable neighbour = randGossip.Next(1, numNodes + 1)
                                while neighbour = ID do
                                    neighbour <- randGossip.Next(1, numNodes + 1)
                                curSum <- curSum / 2.0
                                curWeight <- curWeight / 2.0
                                prevRatio <- curSum / curWeight
                                globalWorkerList.Item((neighbour - 1)) <! MessageToNodeSum(curSum, curWeight, numNodes, ID)
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(1.0), mailbox.Self, WakeTheNode(1))
                        | ShutdownMessage(msg) ->
                            if recivedShutdown = 0 then
                                recivedShutdown <- 1
                                dispatcher <! Finished(sprintf "Got the shutdown message")
                        | _ ->  printfn "An error occured at worker %d" ID
                        return! loop()
                    }
                loop()

let DispatcherFullSum(mailbox: Actor<_>) = 
            let mutable numNodes = 0
            let rec loop() = actor {
                    mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 10000.0)
                    let! message = mailbox.Receive()
                    match message with
                    | DispatcherInput(n, x) ->
                        numNodes <- n
                        for i in 1 .. numNodes do
                            globalWorkerList.Add(spawn system (sprintf "Worker_%d" i) workerFullSum)
                        let neighbour = randGossip.Next(1, numNodes + 1)
                        for j in 0 .. x - 1 do
                            let mutable node = randGossip.Next(1, numNodes + 1)
                            while node = neighbour do
                                node <- randGossip.Next(1, numNodes + 1)
                            globalWorkerList.Item(node - 1) <! ShutdownMessage("Sutdown! Your work is done my child!")
                        globalWorkerList.Item(neighbour - 1) <! MessageToWorkerFullSum((numNodes, neighbour, float(neighbour), 1.0))
                        for j in 0 .. numNodes - 1 do
                            if j <> neighbour - 1 then 
                                timer.Start()
                                globalWorkerList.Item(j) <! MessageToWorkerFullSum((-100, j + 1, float(j + 1), 1.0))
                    | Finished(output) ->
                        countResponce <- countResponce + 1
                        if countResponce <= numNodes then
                            // printfn "%s"output
                            if countResponce = numNodes then
                                printfn "Time taken = %i milliseconds\n" timer.ElapsedMilliseconds
                                mailbox.Context.System.Terminate() |> ignore
                    | _ -> printfn "An error occured at dipatcher"
                    return! loop()
                }
            loop()

let workerLineSum(mailbox: Actor<_>) = 
                let mutable dispatcher = mailbox.Self
                let mutable numNodes = 0
                let mutable ID = -100
                let mutable curSum = 0.0
                let mutable curWeight = 1.0 
                let mutable prevRatio = curSum / curWeight
                let mutable countRatioNotChanged = 0
                let mutable workerDone = false
                let mutable recivedShutdown = 0
                let rec loop() = actor {
                        mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 1000.0)
                        let! message = mailbox.Receive()
                        if dispatcher = mailbox.Self then 
                            dispatcher <- mailbox.Sender()
                        match message with
                        | MessageToWorkerLineSum(nodNum, nodeID, initSum, initWeight) -> 
                            // printfn "I am an actor and I have this message (%d, %d, %f, %f)\n" nodNum nodeID initSum initWeight
                            numNodes <- nodNum
                            ID <- nodeID
                            curSum <- initSum
                            curWeight <- initWeight
                            prevRatio <- curSum / curWeight
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(1.0), mailbox.Self, WakeTheNode(1))
                        | MessageToNodeSum(theSum, theWeight, msg, senderID) ->
                            if recivedShutdown = 0 then
                                numNodes <- msg
                                curSum <- curSum  + theSum
                                curWeight <- curWeight + theWeight
                                let newRatio = curSum / curWeight
                                // if randGossip.Next() % 10 = 9 then
                                // printfn "%f" newRatio 
                                if abs(prevRatio - newRatio) <= float(pown 10.0 -10) then
                                    countRatioNotChanged <- countRatioNotChanged + 1
                                    if countRatioNotChanged >= 3 && not workerDone then
                                        let ratioDiff = abs(prevRatio - newRatio)
                                        dispatcher <! Finished(sprintf "Got %d messages!" 10)
                                        workerDone <- true                                 
                                else 
                                    countRatioNotChanged <- 0
                                prevRatio <- curSum / curWeight
                        | WakeTheNode(com) ->
                            if numNodes <> -100 && recivedShutdown = 0 then
                                let mutable neighbour = [-1; 1].[randGossip.Next() % 2]
                                while (ID + neighbour = ID) || (ID + neighbour < 1) || (ID + neighbour > numNodes) do
                                    neighbour <- [-1; 1].[randGossip.Next() % 2]
                                curSum <- curSum / 2.0
                                curWeight <- curWeight / 2.0
                                prevRatio <- curSum / curWeight
                                globalWorkerList.Item((ID + neighbour - 1)) <! MessageToNodeSum(curSum, curWeight, numNodes, ID)
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(1.0), mailbox.Self, WakeTheNode(1))
                        | ShutdownMessage(msg) ->
                            if recivedShutdown = 0 then
                                recivedShutdown <- 1
                                dispatcher <! Finished(sprintf "Got the shutdown message")
                        | _ ->  printfn "An error occured at worker %d" ID
                        return! loop()
                    }
                loop()

let DispatcherLineSum(mailbox: Actor<_>) = 
            let mutable numNodes = 0
            let rec loop() = actor {
                    mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 10000.0)
                    let! message = mailbox.Receive()
                    match message with
                    | DispatcherInput(n, x) ->
                        numNodes <- n
                        for i in 1 .. numNodes do
                            globalWorkerList.Add(spawn system (sprintf "Worker_%d" i) workerLineSum)
                        let neighbour = randGossip.Next(1, numNodes + 1)
                        for j in 0 .. x - 1 do
                            let mutable node = randGossip.Next(1, numNodes + 1)
                            while node = neighbour do
                                node <- randGossip.Next(1, numNodes + 1)
                            globalWorkerList.Item(node - 1) <! ShutdownMessage("Sutdown! Your work is done my child!")
                        globalWorkerList.Item(neighbour - 1) <! MessageToWorkerLineSum((numNodes, neighbour, float(neighbour), 1.0))
                        for j in 0 .. numNodes - 1 do
                            if j <> neighbour - 1 then 
                                globalWorkerList.Item(j) <! MessageToWorkerLineSum((-100, j + 1, float(j + 1), 1.0))
                    | Finished(output) ->
                        countResponce <- countResponce + 1
                        if countResponce <= numNodes then
                            // printfn "%s"output
                            if countResponce = numNodes then
                                printfn "Time taken = %i milliseconds\n" timer.ElapsedMilliseconds
                                mailbox.Context.System.Terminate() |> ignore
                    | _ -> printfn "An error occured at dipatcher"
                    return! loop()
                }
            loop()

let worker3DSum(mailbox: Actor<_>) = 
                let mutable dispatcher = mailbox.Self
                let mutable numNodes = 0
                let mutable ID = -100
                let mutable curSum = 0.0
                let mutable curWeight = 1.0 
                let mutable prevRatio = curSum / curWeight
                let mutable countRatioNotChanged = 0
                let mutable workerDone = false
                let mutable  neigh1, neigh2, neigh3, neigh4, neigh5, neigh6 = -1, -1, -1, -1, -1, -1
                let mutable recivedShutdown = 0
                let rec loop() = actor {
                        // mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 1000.0)
                        let! message = mailbox.Receive()
                        if dispatcher = mailbox.Self then 
                            dispatcher <- mailbox.Sender()
                        match message with
                        | MessageToWorker3DSum(nodNum, nodeID, nig1, nig2, nig3, nig4, nig5, nig6, initSum, initWeight) -> 
                            // printfn "I am an actor and I have this message (%d, %d, %f, %f)\n" nodNum nodeID initSum initWeight
                            numNodes <- nodNum
                            ID <- nodeID
                            neigh1 <- nig1
                            neigh2 <- nig2
                            neigh3 <- nig3
                            neigh4 <- nig4
                            neigh5 <- nig5
                            neigh6 <- nig6
                            curSum <- initSum
                            curWeight <- initWeight
                            prevRatio <- curSum / curWeight
                            countRatioNotChanged <- 0
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(1.0), mailbox.Self, WakeTheNode(1))
                        | MessageToNodeSum(theSum, theWeight, msg, senderID) ->
                            if recivedShutdown = 0 then
                                numNodes <- msg
                                curSum <- curSum  + theSum
                                curWeight <- curWeight + theWeight
                                let newRatio = curSum / curWeight
                                // if randGossip.Next() % 10 = 9 then
                                // printfn "%f" newRatio 
                                if abs(prevRatio - newRatio) <= float(pown 10.0 -10) then
                                    countRatioNotChanged <- countRatioNotChanged + 1
                                    if countRatioNotChanged >= 3 && not workerDone then
                                        let ratioDiff = abs(prevRatio - newRatio)
                                        dispatcher <! Finished(sprintf "Got %d messages!" 10)
                                        workerDone <- true                                 
                                else 
                                    countRatioNotChanged <- 0
                                prevRatio <- curSum / curWeight
                        | WakeTheNode(com) ->
                            if numNodes <> -100 && recivedShutdown = 0 then
                                let mutable neighbour = [neigh1; neigh2; neigh3; neigh4; neigh5; neigh6].[randGossip.Next() % 6]
                                while neighbour = -1 do
                                    neighbour <- [neigh1; neigh2; neigh3; neigh4; neigh5; neigh6; neigh6].[randGossip.Next() % 6]
                                curSum <- curSum / 2.0
                                curWeight <- curWeight / 2.0
                                prevRatio <- curSum / curWeight
                                globalWorkerList.Item((neighbour - 1)) <! MessageToNodeSum(curSum, curWeight, numNodes, ID)
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(1.0), mailbox.Self, WakeTheNode(1))
                        | ShutdownMessage(msg) ->
                            if recivedShutdown = 0 then
                                recivedShutdown <- 1
                                dispatcher <! Finished(sprintf "Got the shutdown message")
                        | _ ->  printfn "An error occured at worker %d" ID
                        return! loop()
                    }
                loop()

let Dispatcher3DSum(mailbox: Actor<_>) = 
            let mutable numNodes = 0
            let rec loop() = actor {
                    mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 10000.0)
                    let! message = mailbox.Receive()
                    match message with
                    | DispatcherInput(n, x) ->
                        numNodes <- n
                        for i in 1 .. numNodes do
                            globalWorkerList.Add(spawn system (sprintf "Worker_%d" i) worker3DSum)
                        let dimension, tempDimension = threeDimensionDims(float(numNodes))
                        let nodeThattKnows = randGossip.Next(1, numNodes + 1)
                        let neighbourList = threeDimension dimension tempDimension (float(nodeThattKnows)) (float(numNodes))
                        let neigh1, neigh2, neigh3, neigh4, neigh5, neigh6 = AssignNeighbour neighbourList
                        for j in 0 .. x - 1 do
                            let mutable node = randGossip.Next(1, numNodes + 1)
                            while node = nodeThattKnows do
                                node <- randGossip.Next(1, numNodes + 1)
                            globalWorkerList.Item(node - 1) <! ShutdownMessage("Sutdown! Your work is done my child!")
                        globalWorkerList.Item(nodeThattKnows - 1) <! MessageToWorker3DSum((numNodes, nodeThattKnows, neigh1, neigh2, neigh3, neigh4, neigh5, neigh6, float(nodeThattKnows), 1.0))
                        for j in 0 .. numNodes - 1 do
                            if j <> nodeThattKnows - 1 then
                                let neighbourList = threeDimension dimension tempDimension (float(j + 1)) (float(numNodes))
                                let neigh1, neigh2, neigh3, neigh4, neigh5, neigh6 = AssignNeighbour neighbourList
                                timer.Start()
                                globalWorkerList.Item(j) <! MessageToWorker3DSum((-100, j + 1, neigh1, neigh2, neigh3, neigh4, neigh5, neigh6, float(j + 1), 1.0))
                    | Finished(output) ->
                        countResponce <- countResponce + 1
                        // printfn "Num of workers done - %i" countResponce
                        if countResponce <= numNodes then
                            // printfn "%s"output
                            if countResponce = numNodes then
                                printfn "Time taken = %i milliseconds\n" timer.ElapsedMilliseconds
                                mailbox.Context.System.Terminate() |> ignore
                    | _ -> printfn "An error occured at dipatcher"
                    return! loop()
                }
            loop()

let workerImpe3DSum(mailbox: Actor<_>) = 
                let mutable dispatcher = mailbox.Self
                let mutable numNodes = 0
                let mutable ID = -100
                let mutable curSum = 0.0
                let mutable curWeight = 1.0 
                let mutable prevRatio = curSum / curWeight
                let mutable countRatioNotChanged = 0
                let mutable workerDone = false
                let mutable  neigh1, neigh2, neigh3, neigh4, neigh5, neigh6, neigh7= -1, -1, -1, -1, -1, -1, -1
                let mutable recivedShutdown = 0
                let rec loop() = actor {
                        let! message = mailbox.Receive()
                        if dispatcher = mailbox.Self then 
                            dispatcher <- mailbox.Sender()
                        match message with
                        | MessageToWorkerImpe3DSum(nodNum, nodeID, nig1, nig2, nig3, nig4, nig5, nig6, nig7, initSum, initWeight) -> 
                            // printfn "I am an actor and I have this message (%d, %d, %f, %f)\n" nodNum nodeID initSum initWeight
                            numNodes <- nodNum
                            ID <- nodeID
                            neigh1 <- nig1
                            neigh2 <- nig2
                            neigh3 <- nig3
                            neigh4 <- nig4
                            neigh5 <- nig5
                            neigh6 <- nig6
                            neigh7 <- nig7
                            curSum <- initSum
                            curWeight <- initWeight
                            prevRatio <- curSum / curWeight
                            countRatioNotChanged <- 0
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(1.0), mailbox.Self, WakeTheNode(1))
                        | MessageToNodeSum(theSum, theWeight, msg, senderID) ->
                            if recivedShutdown = 0 then
                                numNodes <- msg
                                curSum <- curSum  + theSum
                                curWeight <- curWeight + theWeight
                                let newRatio = curSum / curWeight
                                // if randGossip.Next() % 10 = 9 then
                                // printfn "%f" newRatio 
                                if abs(prevRatio - newRatio) <= float(pown 10.0 -10) then
                                    countRatioNotChanged <- countRatioNotChanged + 1
                                    if countRatioNotChanged >= 3 && not workerDone then
                                        let ratioDiff = abs(prevRatio - newRatio)
                                        dispatcher <! Finished(sprintf "Got %d messages!" 10)
                                        workerDone <- true
                                        // printfn "%f" newRatio                               
                                else 
                                    countRatioNotChanged <- 0
                                prevRatio <- curSum / curWeight
                        | WakeTheNode(com) ->
                            if numNodes <> -100 && recivedShutdown = 0 then
                                let mutable neighbour = [neigh1; neigh2; neigh3; neigh4; neigh5; neigh6; neigh7].[randGossip.Next() % 7]
                                while neighbour = -1 do
                                    neighbour <- [neigh1; neigh2; neigh3; neigh4; neigh5; neigh6; neigh6; neigh7].[randGossip.Next() % 7]
                                curSum <- curSum / 2.0
                                curWeight <- curWeight / 2.0
                                prevRatio <- curSum / curWeight
                                globalWorkerList.Item((neighbour - 1)) <! MessageToNodeSum(curSum, curWeight, numNodes, ID)
                            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(1.0), mailbox.Self, WakeTheNode(1))
                        | ShutdownMessage(msg) ->
                            if recivedShutdown = 0 then
                                recivedShutdown <- 1
                                dispatcher <! Finished(sprintf "Got the shutdown message")
                        | _ ->  printfn "An error occured at worker %d" ID
                        return! loop()
                    }
                loop()

let DispatcherImpe3DSum(mailbox: Actor<_>) = 
            let mutable numNodes = 0
            let rec loop() = actor {
                    mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 10000.0)
                    let! message = mailbox.Receive()
                    match message with
                    | DispatcherInput(n, x) ->
                        numNodes <- n
                        for i in 1 .. numNodes do
                            globalWorkerList.Add(spawn system (sprintf "Worker_%d" i) workerImpe3DSum)
                            globalWorkerDoneList.Add(0)
                        let dimension, tempDimension = threeDimensionDims(float(numNodes))
                        let nodeThattKnows = randGossip.Next(1, numNodes + 1)
                        let neighbourList = threeDimension dimension tempDimension (float(nodeThattKnows)) (float(numNodes))
                        let neigh1, neigh2, neigh3, neigh4, neigh5, neigh6 = AssignNeighbour neighbourList
                        let mutable neigh7 = randGossip.Next(1, n + 1)
                        while neigh7 = nodeThattKnows do
                            neigh7 <- randGossip.Next(1, n + 1)
                        for j in 0 .. x - 1 do
                            let mutable node = randGossip.Next(1, numNodes + 1)
                            while node = nodeThattKnows do
                                node <- randGossip.Next(1, numNodes + 1)
                            globalWorkerList.Item(node - 1) <! ShutdownMessage("Sutdown! Your work is done my child!")
                        globalWorkerList.Item(nodeThattKnows - 1) <! MessageToWorkerImpe3DSum((numNodes, nodeThattKnows, neigh1, neigh2, neigh3, neigh4, neigh5, neigh6, neigh7, float(nodeThattKnows), 1.0))
                        for j in 0 .. numNodes - 1 do
                            if j <> nodeThattKnows - 1 then
                                let neighbourList = threeDimension dimension tempDimension (float(j + 1)) (float(numNodes))
                                let neigh1, neigh2, neigh3, neigh4, neigh5, neigh6 = AssignNeighbour neighbourList
                                let mutable neigh7 = randGossip.Next(1, n + 1)
                                while neigh7 = j + 1  do
                                    neigh7 <- randGossip.Next(1, n + 1)
                                timer.Start()
                                globalWorkerList.Item(j) <! MessageToWorkerImpe3DSum((-100, j + 1, neigh1, neigh2, neigh3, neigh4, neigh5, neigh6, neigh7, float(j + 1), 1.0))

                    | Finished(output) ->
                        countResponce <- countResponce + 1
                        // printfn "Num of workers done - %d" countResponce
                        if countResponce <= numNodes then
                            // printfn "%s"output
                            if countResponce = numNodes then
                                printfn "Time taken = %i  milliseconds\n" timer.ElapsedMilliseconds
                                mailbox.Context.System.Terminate() |> ignore
                    | _ -> printfn "An error occured at dipatcher"
                    return! loop()
                }
            loop()

let N = fsi.CommandLineArgs.[1] |>int
let topology = fsi.CommandLineArgs.[2] |>string
let algo = fsi.CommandLineArgs.[3] |>string
let deadNodeCont = fsi.CommandLineArgs.[4] |>int
if topology = "line" then
    if algo = "gossip" then
        let Boss  = spawn system "Boss" DispatcherLine
        Boss <! DispatcherInput(N, deadNodeCont)
    if algo = "push-sum" then
        let Boss  = spawn system "Boss" DispatcherLineSum
        Boss <! DispatcherInput(N, deadNodeCont)
else if topology = "full" then
    if algo = "gossip" then
        let Boss  = spawn system "Boss" DispatcherFull
        Boss <! DispatcherInput(N, deadNodeCont)
    if algo = "push-sum" then
        let Boss  = spawn system "Boss" DispatcherFullSum
        Boss <! DispatcherInput(N, deadNodeCont)
else if topology = "3D" then
    if algo = "gossip" then
        let Boss  = spawn system "Boss" Dispatcher3D
        Boss <! DispatcherInput(N, deadNodeCont)
    if algo = "push-sum" then
        let Boss  = spawn system "Boss" Dispatcher3DSum
        Boss <! DispatcherInput(N, deadNodeCont)
else if topology = "imp3D" then
    if algo = "gossip" then
        let Boss  = spawn system "Boss" DispatcherImpe3D
        Boss <! DispatcherInput(N, deadNodeCont)
    if algo = "push-sum" then
        let Boss  = spawn system "Boss" DispatcherImpe3DSum
        Boss <! DispatcherInput(N, deadNodeCont)
else
    printfn "The selected topology is not recognized. Please select from one of the following topologies:\nline\nfull\n3D\nimp3D"

system.WhenTerminated.Wait()
//system.Terminate() 