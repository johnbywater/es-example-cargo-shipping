from datetime import datetime
from typing import List, Optional
from uuid import UUID

from eventsourcing.application.process import ProcessApplication
from eventsourcing.domain.model.aggregate import TAggregate, TAggregateEvent

from cargoshipping.domainmodel import (
    Cargo,
    HandlingActivity,
    Itinerary,
    Location,
    REGISTERED_ROUTES,
)

# Cargo aggregates exist within an application, which
# provides "application service" methods for clients.
class BookingApplication(ProcessApplication[TAggregate, TAggregateEvent]):
    persist_event_type = Cargo.Event

    @staticmethod
    def book_new_cargo(
        origin: Location, destination: Location, arrival_deadline: datetime
    ) -> UUID:
        cargo = Cargo.new_booking(origin, destination, arrival_deadline)
        cargo.__save__()
        return cargo.id

    def change_destination(self, tracking_id: UUID, destination: Location) -> None:
        cargo = self.get_cargo(tracking_id)
        cargo.change_destination(destination)
        cargo.__save__()

    def request_possible_routes_for_cargo(self, tracking_id: UUID) -> List[Itinerary]:
        cargo = self.get_cargo(tracking_id)
        from_location = (cargo.last_known_location or cargo.origin).value
        to_location = cargo.destination.value
        try:
            possible_routes = REGISTERED_ROUTES[(from_location, to_location)]
        except KeyError:
            raise Exception(
                "Can't find routes from {} to {}".format(from_location, to_location)
            )

        return possible_routes

    def assign_route(self, tracking_id: UUID, itinerary: Itinerary) -> None:
        cargo = self.get_cargo(tracking_id)
        cargo.assign_route(itinerary)
        cargo.__save__()

    def register_handling_event(
        self,
        tracking_id: UUID,
        voyage_number: Optional[str],
        location: Location,
        handing_activity: HandlingActivity,
    ) -> None:
        cargo = self.get_cargo(tracking_id)
        cargo.register_handling_event(
            tracking_id, voyage_number, location, handing_activity
        )
        cargo.__save__()

    def get_cargo(self, tracking_id: UUID) -> Cargo:
        cargo = self.repository.get_instance_of(Cargo, tracking_id)
        if cargo is None:
            raise Exception("Cargo not found: {}".format(tracking_id))
        else:
            return cargo
